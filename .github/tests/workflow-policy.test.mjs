import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { parse, stringify } from "yaml";

const root = fileURLToPath(new URL("../../", import.meta.url));
const checker = fileURLToPath(new URL("workflow-policy.mjs", import.meta.url));

const regressions = [
  [
    "a validation job hardcodes a hosted runner",
    (ci) => {
      ci.jobs.check["runs-on"] = "ubuntu-latest";
    },
  ],
  [
    "a matrix entry hardcodes a hosted runner",
    (ci) => {
      ci.jobs.e2e.strategy.matrix.include.find(
        (row) => row.os === "Windows",
      ).runner = "windows-latest";
    },
  ],
  [
    "a job bypasses its configured runner matrix",
    (ci) => {
      ci.jobs.e2e["runs-on"] = "ubuntu-latest";
    },
  ],
  [
    "a new helper job hardcodes a hosted runner",
    (ci) => {
      ci.jobs["new-helper"] = {
        "runs-on": "ubuntu-latest",
        steps: [{ run: "true" }],
      };
    },
  ],
];

for (const [name, mutate] of regressions) {
  test(name, () => {
    const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "kache-policy-"));
    try {
      fs.cpSync(
        path.join(root, ".github/workflows"),
        path.join(fixture, ".github/workflows"),
        { recursive: true },
      );
      const ciPath = path.join(fixture, ".github/workflows/ci.yml");
      const ci = parse(fs.readFileSync(ciPath, "utf8"));
      mutate(ci);
      fs.writeFileSync(ciPath, stringify(ci));

      const result = spawnSync(process.execPath, [checker, fixture], {
        encoding: "utf8",
        timeout: 10_000,
      });
      assert.ifError(result.error);
      assert.equal(result.status, 1, result.stdout + result.stderr);
      assert.match(result.stderr, /AssertionError.*private configured/);
    } finally {
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  });
}
