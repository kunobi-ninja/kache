#!/usr/bin/env python3
"""Verify that the restore-reason regressions fail under actual regressions."""

import argparse
import json
import os
from pathlib import Path
import subprocess


def run_test(command, destination):
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = result.stdout.decode("utf-8", errors="replace")
    destination.write_text(output)
    print(output, flush=True)
    return result.returncode, output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", required=True, choices=("persisted", "warning"))
    args = parser.parse_args()
    evidence = Path(os.environ["EVIDENCE_DIR"])
    evidence.mkdir(parents=True, exist_ok=True)
    source = Path("src/wrapper.rs")
    original = source.read_text()
    if args.case == "persisted":
        before = 'format!("restore failed: {}", store_error_for_event(error))'
        after = 'format!("restore failed: {error:#}")'
        test = "wrapper::tests::restore_failure_reasons_flatten_archive_text"
        command = ["cargo", "test", "--locked", "--lib", test, "--", "--exact"]
        failure_marker = "assertion `left == right` failed"
    else:
        if os.uname().sysname != "Darwin":
            raise RuntimeError("The real debug-bundle warning control requires macOS")
        before = (
            '"restoring local cache hit for {} failed: {} — recompiling",\n'
            "                            crate_name,\n"
            "                            store_error_for_event(&e)"
        )
        after = (
            '"restoring local cache hit for {} failed: {:#} — recompiling",\n'
            "                            crate_name,\n"
            "                            e"
        )
        test = "a_bundle_that_fails_to_unpack_recompiles_and_records_the_cause"
        command = [
            "cargo", "test", "--locked", "--test", "macos_debug_bundle_policy_test",
            test, "--", "--exact", "--test-threads=1",
        ]
        failure_marker = "restore warning contains an unflattened archive name"
    if original.count(before) != 1:
        raise RuntimeError(f"Expected one mutation site for {args.case}")
    try:
        source.write_text(original.replace(before, after))
        code, output = run_test(command, evidence / f"{args.case}-mutant.log")
        if code == 0 or "test result: FAILED." not in output or failure_marker not in output:
            raise RuntimeError(f"{args.case} regression was not caught by the intended assertion")
    finally:
        source.write_text(original)
    code, output = run_test(command, evidence / f"{args.case}-restored.log")
    if code != 0 or "test result: ok. 1 passed" not in output:
        raise RuntimeError(f"{args.case} positive control did not pass after restoration")
    (evidence / f"{args.case}-control.json").write_text(json.dumps({
        "case": args.case, "test": test, "mutant": "caught", "restored": "passed",
    }, indent=2) + "\n")


if __name__ == "__main__":
    main()
