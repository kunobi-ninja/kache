import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { ensureNew } from "./check_existing.mjs";

test("an exact existing package version stops publication", () => {
  for (const state of ["OPEN", "CLOSED", "MERGED"]) {
    assert.throws(
      () =>
        ensureNew(
          [
            {
              title: "New version: kunobi-ninja.kache version 1.2.3",
              url: "https://github.com/microsoft/winget-pkgs/pull/1",
              state,
            },
          ],
          "kunobi-ninja.kache",
          "1.2.3",
        ),
      /already exists/,
    );
  }
});

test("package and version prefixes do not block another release", () => {
  ensureNew(
    [
      { title: "New version: kunobi-ninja.kache.Unstable version 1.2.3" },
      { title: "New version: kunobi-ninja.kache version 1.2.3-rc.1" },
      { title: "New version: other.kache version 1.2.3" },
      { title: "New version: kunobi-ninja.kache version 11.2.3" },
    ],
    "kunobi-ninja.kache",
    "1.2.3",
  );
  ensureNew([], "kunobi-ninja.kache", "1.2.3");
});

test("CLI rejects malformed search output and existing pull requests", () => {
  const script = fileURLToPath(new URL("check_existing.mjs", import.meta.url));
  for (const [input, status] of [
    [[], 0],
    [
      [
        {
          title: "kunobi-ninja.kache 1.2.3",
          url: "https://github.com/microsoft/winget-pkgs/pull/1",
        },
      ],
      1,
    ],
    [{ error: "API unavailable" }, 1],
  ]) {
    const result = spawnSync(
      process.execPath,
      [script, "--package-id", "kunobi-ninja.kache", "--version", "1.2.3"],
      { input: JSON.stringify(input), encoding: "utf8", timeout: 10_000 },
    );
    assert.ifError(result.error);
    assert.equal(result.status, status, result.stderr);
  }
});
