#!/usr/bin/env python3
"""Write the Rust diff the changed-line mutation gate mutates.

`git diff` shows code moved to another place as removed and added again, so
splitting a large file would put every moved function in the gate. This
writes the same diff, but each added line that git marks as moved, unchanged
apart from its indentation, becomes a context line. Its text stays in the
diff, which cargo-mutants checks against the tree; only lines the change
wrote or edited count as changed.

Moved lines come from git's own detection (`--color-moved=blocks`): a moved
block needs at least 20 alphanumeric characters, so a lone brace or a common
one-liner that happens to appear elsewhere is still treated as new. When the
colored and plain diffs do not line up, the plain diff is written unchanged,
so the gate never mutates less than it did.

Usage: mutation-diff.py BASE HEAD > pr.diff
"""

import re
import subprocess
import sys

DIFF_ARGS = (
    "diff",
    "--no-ext-diff",
    "--diff-algorithm=histogram",
    "--unified=1",
)
# A color no other part of the diff uses, so a moved added line is known by
# the escape it starts with.
MOVED_COLOR = "magenta ul"
HUNK = re.compile(r"^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@(.*)$")
ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


def git(*args):
    return subprocess.run(
        ("git", *args), check=True, capture_output=True, text=True
    ).stdout


def moved_escape():
    return git("config", "--get-color", "", MOVED_COLOR)


def colored_args():
    moved = [
        f"color.diff.{slot}={MOVED_COLOR}"
        for slot in (
            "newMoved",
            "newMovedAlternative",
            "newMovedDimmed",
            "newMovedAlternativeDimmed",
        )
    ]
    config = [part for setting in moved for part in ("-c", setting)]
    return (
        *config,
        *DIFF_ARGS,
        "--color=always",
        "--color-moved=blocks",
        "--color-moved-ws=allow-indentation-change",
    )


def moved_added_lines(plain, colored, escape):
    """Indexes of the plain diff's added lines that git colored as moved, or
    None when the two diffs do not describe the same lines."""
    if len(plain) != len(colored):
        return None
    moved = set()
    for index, (line, painted) in enumerate(zip(plain, colored)):
        if ESCAPE.sub("", painted) != line:
            return None
        if line.startswith("+") and not line.startswith("+++") and painted.startswith(escape):
            moved.add(index)
    return moved


def rewrite(plain, moved):
    """The plain diff with moved added lines as context, hunk counts redone,
    and hunks or files left with nothing changed dropped."""
    out = []
    header = []
    hunk = None

    def flush_hunk():
        nonlocal hunk
        if hunk is None:
            return
        start, lines = hunk
        hunk = None
        if not any(line[:1] in "+-" for line in lines):
            return
        old = sum(1 for line in lines if line[:1] in " -")
        new = sum(1 for line in lines if line[:1] in " +")
        old_start, new_start, tail = start
        if header:
            out.extend(header)
            header.clear()
        out.append(f"@@ -{old_start},{old} +{new_start},{new} @@{tail}")
        out.extend(lines)

    for index, line in enumerate(plain):
        if line.startswith("diff --git "):
            flush_hunk()
            header[:] = [line]
            continue
        match = HUNK.match(line)
        if match:
            flush_hunk()
            hunk = ((match.group(1), match.group(2), match.group(3)), [])
            continue
        if hunk is None:
            header.append(line)
            continue
        if index in moved:
            line = " " + line[1:]
        hunk[1].append(line)
    flush_hunk()
    return out


def main(argv):
    if len(argv) != 3:
        sys.exit(__doc__.strip().splitlines()[-1])
    base, head = argv[1], argv[2]
    paths = ("--", "*.rs")
    plain = git(*DIFF_ARGS, "--no-color", base, head, *paths).splitlines()
    colored = git(*colored_args(), base, head, *paths).splitlines()
    moved = moved_added_lines(plain, colored, moved_escape())
    if moved is None:
        print("mutation-diff: colored diff did not line up; keeping every line", file=sys.stderr)
        moved = set()
    added = sum(1 for line in plain if line.startswith("+") and not line.startswith("+++"))
    print(f"mutation-diff: {len(moved)} of {added} added lines are moved", file=sys.stderr)
    sys.stdout.writelines(line + "\n" for line in rewrite(plain, moved))


if __name__ == "__main__":
    main(sys.argv)
