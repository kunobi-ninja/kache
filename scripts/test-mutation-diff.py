#!/usr/bin/env python3
"""The mutation diff keeps new and edited lines and drops moved ones."""

import importlib.util
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest

SCRIPT = Path(__file__).with_name("mutation-diff.py")
spec = importlib.util.spec_from_file_location("mutation_diff", SCRIPT)
md = importlib.util.module_from_spec(spec)
spec.loader.exec_module(md)

MOVED = textwrap.dedent(
    """\
    pub fn checksum_of_every_record(records: &[u64]) -> u64 {
        let mut total = 0u64;
        for record in records {
            total = total.wrapping_mul(31).wrapping_add(*record);
        }
        total
    }
    """
)
KEPT = textwrap.dedent(
    """\
    pub fn kept_where_it_was(value: u64) -> u64 {
        value.saturating_add(1)
    }
    """
)


class Repo:
    def __init__(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = Path(self.dir.name)
        self.git("init", "-q")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "user.name", "test")
        self.git("config", "commit.gpgsign", "false")

    def git(self, *args):
        return subprocess.run(
            ("git", *args), cwd=self.path, check=True, capture_output=True, text=True
        ).stdout

    def commit(self, files):
        for name, text in files.items():
            path = self.path / name
            if text is None:
                path.unlink()
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "change")
        return self.git("rev-parse", "HEAD").strip()

    def mutation_diff(self, base, head):
        return subprocess.run(
            (sys.executable, str(SCRIPT), base, head),
            cwd=self.path,
            check=True,
            capture_output=True,
            text=True,
        ).stdout


def added(diff):
    return [line[1:] for line in diff.splitlines() if line.startswith("+") and not line.startswith("+++")]


def check_counts(test, diff):
    """Every hunk header counts exactly the lines under it."""
    lines = diff.splitlines()
    for index, line in enumerate(lines):
        match = re.match(r"^@@ -\d+,(\d+) \+\d+,(\d+) @@", line)
        if not match:
            continue
        body = []
        for following in lines[index + 1 :]:
            if following.startswith(("@@", "diff --git ")):
                break
            body.append(following)
        test.assertEqual(int(match.group(1)), sum(1 for b in body if b[:1] in (" ", "-")), line)
        test.assertEqual(int(match.group(2)), sum(1 for b in body if b[:1] in (" ", "+")), line)


class MutationDiffTests(unittest.TestCase):
    def setUp(self):
        self.repo = Repo()
        self.base = self.repo.commit({"src/a.rs": KEPT + "\n" + MOVED})

    def test_a_function_moved_into_a_module_elsewhere_is_not_changed(self):
        indented = textwrap.indent(MOVED, "    ")
        head = self.repo.commit(
            {"src/a.rs": KEPT, "src/b.rs": "pub mod inner {\n" + indented + "}\n"}
        )
        diff = self.repo.mutation_diff(self.base, head)
        self.assertEqual(added(diff), ["pub mod inner {", "}"])
        self.assertIn(" " + indented.splitlines()[0], diff.splitlines())
        check_counts(self, diff)

    def test_an_edited_line_in_moved_code_stays_changed(self):
        edited = MOVED.replace("wrapping_mul(31)", "wrapping_mul(37)")
        head = self.repo.commit({"src/a.rs": KEPT, "src/b.rs": edited})
        diff = self.repo.mutation_diff(self.base, head)
        changed = added(diff)
        self.assertIn("        total = total.wrapping_mul(37).wrapping_add(*record);", changed)
        # The unchanged head of the function is moved. A tail too short to
        # be a moved block by itself stays changed, which only puts an
        # edited function in the gate.
        self.assertNotIn(MOVED.splitlines()[0], changed)
        self.assertNotIn(MOVED.splitlines()[1], changed)
        check_counts(self, diff)

    def test_new_code_is_changed(self):
        new = "pub fn brand_new_function_with_a_body() -> u32 {\n    40 + 2\n}\n"
        head = self.repo.commit({"src/a.rs": KEPT + "\n" + MOVED + "\n" + new})
        diff = self.repo.mutation_diff(self.base, head)
        self.assertEqual(added(diff), ["", *new.splitlines()])
        check_counts(self, diff)

    def test_a_pure_removal_keeps_its_hunk(self):
        head = self.repo.commit({"src/a.rs": KEPT})
        diff = self.repo.mutation_diff(self.base, head)
        self.assertEqual(added(diff), [])
        self.assertIn("-" + MOVED.splitlines()[0], diff.splitlines())
        check_counts(self, diff)

    def test_only_rust_files_are_diffed(self):
        head = self.repo.commit({"notes.txt": "not rust\n"})
        self.assertEqual(self.repo.mutation_diff(self.base, head), "")


class AlignmentTests(unittest.TestCase):
    ESC = "\x1b[4;35m"

    def test_moved_lines_are_the_painted_additions(self):
        plain = ["diff --git a/x b/x", "@@ -1,1 +1,2 @@", " a", "+b", "+c"]
        colored = [plain[0], plain[1], " a", self.ESC + "+b\x1b[m", "\x1b[32m+c\x1b[m"]
        self.assertEqual(md.moved_added_lines(plain, colored, self.ESC), {3})

    def test_diffs_that_do_not_line_up_move_nothing(self):
        plain = ["@@ -1,1 +1,1 @@", "+b"]
        self.assertIsNone(md.moved_added_lines(plain, plain[:1], self.ESC))
        self.assertIsNone(md.moved_added_lines(plain, [plain[0], "+c"], self.ESC))


if __name__ == "__main__":
    unittest.main()
