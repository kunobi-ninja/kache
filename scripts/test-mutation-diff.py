#!/usr/bin/env python3
"""The mutation diff keeps new and edited lines and drops moved ones."""

from collections import Counter
import importlib.util
from pathlib import Path
import random
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


class HunkOrderTests(unittest.TestCase):
    """cargo-mutants refuses a diff whose hunks overlap on either side, checked
    on the header numbers as written."""

    PLAIN = [
        "diff --git a/x.rs b/x.rs",
        "--- a/x.rs",
        "+++ b/x.rs",
        "@@ -1,2 +1,4 @@",
        " a",
        "+moved",
        "+new",
        " b",
        "@@ -3,2 +5,2 @@",
        " c",
        "-d",
        "+e",
    ]

    def test_a_moved_line_pushes_later_hunks_down_on_the_old_side(self):
        out = md.rewrite(self.PLAIN, {5})
        self.assertIn("@@ -1,3 +1,4 @@", out)
        # The moved line lengthened the first hunk's old side by one.
        self.assertIn("@@ -4,2 +5,2 @@", out)
        self.assertTrue(md.well_formed(out))

    def test_without_the_shift_the_hunks_would_overlap(self):
        unshifted = md.rewrite(self.PLAIN, {5})
        unshifted[unshifted.index("@@ -4,2 +5,2 @@")] = "@@ -3,2 +5,2 @@"
        self.assertFalse(md.well_formed(unshifted))

    def test_the_shift_restarts_in_each_file(self):
        second = [line.replace("x.rs", "y.rs") for line in self.PLAIN]
        out = md.rewrite(self.PLAIN + second, {5})
        self.assertEqual(out.count("@@ -3,2 +5,2 @@"), 1, out)
        self.assertTrue(md.well_formed(out))

    def test_a_hunk_whose_count_is_wrong_is_not_well_formed(self):
        self.assertFalse(md.well_formed(["@@ -1,2 +1,1 @@", " a", "+b"]))


def random_function(rng, index):
    """A Rust function long enough for git to track as a moved block."""
    name = f"function_number_{index}_{rng.randrange(10**6)}"
    body = [f"    let value_{i} = input.wrapping_mul({rng.randrange(2, 99)});" for i in range(rng.randrange(2, 7))]
    return [f"pub fn {name}(input: u64) -> u64 {{", *body, "    input", "}"]


def random_change(rng, blocks):
    """Head files built from the base blocks: some moved to another file or
    indented into a module, some edited, some new, some removed."""
    a, b = [], []
    for block in blocks:
        roll = rng.random()
        if roll < 0.25:
            b.append(["pub mod moved {", *("    " + line for line in block), "}"])
        elif roll < 0.4:
            b.append(block)
        elif roll < 0.55:
            edited = list(block)
            edited[rng.randrange(1, len(edited) - 1)] = "    let edited = input + 1;"
            a.append(edited)
        elif roll < 0.65:
            continue
        else:
            a.append(block)
        if rng.random() < 0.3:
            a.append(random_function(rng, 1000 + len(a)))
    rng.shuffle(b)
    # Reorder within the same file too: moved lines then land in hunks next to
    # other changes in that file, which is where hunks can collide.
    if len(a) > 2 and rng.random() < 0.7:
        i, j = rng.sample(range(len(a)), 2)
        a.insert(j, a.pop(i))
    return a, b


def hunks(diff):
    """(file, new_start, new_count, body) for each hunk."""
    current = None
    out = []
    lines = diff.splitlines()
    for index, line in enumerate(lines):
        if line.startswith("+++ "):
            current = line[6:] if line.startswith("+++ b/") else None
            continue
        match = md.HUNK.match(line)
        if not match:
            continue
        body = []
        for following in lines[index + 1 :]:
            if following.startswith(("@@", "diff --git ")):
                break
            body.append(following)
        out.append((current, int(match.group(3)), int(match.group(4) or 1), body))
    return out


def parses_like_cargo_mutants(diff):
    """cargo-mutants' parser rule, written independently of the script:
    counts match each hunk body, and within a file every hunk ends at or
    before the next one starts, on both sides, using the header numbers."""
    previous = None
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            previous = None
            continue
        match = re.match(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", line)
        if not match:
            continue
        old = (int(match.group(1)), int(match.group(2) or 1))
        new = (int(match.group(3)), int(match.group(4) or 1))
        if previous and (previous[0] > old[0] or previous[1] > new[0]):
            return False
        previous = (old[0] + old[1], new[0] + new[1])
    return True


class PropertyTests(unittest.TestCase):
    """For random refactors, the rewritten diff is one cargo-mutants accepts,
    agrees with the files it describes, and only ever drops changed lines."""

    CASES = 40

    def test_rewritten_diffs_are_well_formed_and_match_the_tree(self):
        for seed in range(self.CASES):
            with self.subTest(seed=seed):
                rng = random.Random(seed)
                repo = Repo()
                blocks = [random_function(rng, i) for i in range(rng.randrange(3, 9))]
                base = repo.commit({"src/a.rs": "\n\n".join("\n".join(b) for b in blocks) + "\n"})
                a, b = random_change(rng, blocks)
                files = {"src/a.rs": "\n\n".join("\n".join(x) for x in a) + "\n"}
                if b:
                    files["src/b.rs"] = "\n\n".join("\n".join(x) for x in b) + "\n"
                head = repo.commit(files)
                diff = repo.mutation_diff(base, head)
                plain = repo.git(*md.DIFF_ARGS, "--no-color", base, head, "--", "*.rs")

                self.assertTrue(parses_like_cargo_mutants(diff), diff)
                check_counts(self, diff)
                self.assertLessEqual(Counter(added(diff)), Counter(added(plain)))
                for path, start, count, body in hunks(diff):
                    if path is None or count == 0:
                        continue
                    tree = repo.git("show", f"{head}:{path}").splitlines()
                    new_side = [line[1:] for line in body if line[:1] in " +"]
                    self.assertEqual(tree[start - 1 : start - 1 + count], new_side, (path, start))


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
