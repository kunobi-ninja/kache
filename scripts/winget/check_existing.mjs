#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

export function ensureNew(pullRequests, packageId, version) {
  assert(Array.isArray(pullRequests), "expected a pull request list");
  for (const pullRequest of pullRequests) {
    assert.equal(
      typeof pullRequest.title,
      "string",
      "pull request title is missing",
    );
    const words = pullRequest.title.split(/\s+/u);
    // Match Komac's complete-identifier and complete-version search rule.
    if (words.includes(packageId) && words.includes(version)) {
      throw new Error(`Winget pull request already exists: ${pullRequest.url}`);
    }
  }
}

if (
  process.argv[1] &&
  pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url
) {
  const { values } = parseArgs({
    options: {
      "package-id": { type: "string" },
      version: { type: "string" },
    },
  });
  assert(
    values["package-id"] && values.version,
    "package-id and version are required",
  );
  ensureNew(
    JSON.parse(fs.readFileSync(0, "utf8")),
    values["package-id"],
    values.version,
  );
}
