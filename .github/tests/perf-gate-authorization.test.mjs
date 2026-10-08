import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { parse } from "yaml";

const workflow = parse(
  fs.readFileSync(new URL("../workflows/perf-gate.yml", import.meta.url), "utf8"),
);
// Execute the actual trusted authorization step, so changing its branch gate
// changes these results. No checkout or benchmark commands run in this test.
const authorize = workflow.jobs.authorize.steps.find(
  (step) => step.id === "check",
).run;
const repository = "kunobi-ninja/kache";
const triggerSha = "0123456789abcdef0123456789abcdef01234567";

function pullRequest(overrides = {}) {
  return {
    head: {
      repo: { full_name: repository },
      sha: triggerSha,
      ref: "feature/cache-fix",
    },
    base: { ref: "main" },
    state: "open",
    draft: false,
    title: "Fix a cache lookup",
    labels: [],
    ...overrides,
  };
}

function authorization(pr) {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "kache-perf-auth-"));
  try {
    const executable = path.join(fixture, "gh");
    // The only gh command admitted by the stub reads our fixture, not GitHub.
    fs.writeFileSync(
      executable,
      '#!/bin/sh\n' +
        '[ "$#" -eq 2 ] && [ "$1" = api ] && ' +
        '[ "$2" = "repos/kunobi-ninja/kache/pulls/123" ] || exit 99\n' +
        'exec cat "$PERF_GATE_TEST_PR"\n',
      { mode: 0o700 },
    );
    const apiResponse = path.join(fixture, "pull-request.json");
    const output = path.join(fixture, "output");
    const summary = path.join(fixture, "summary");
    fs.writeFileSync(apiResponse, JSON.stringify(pr));
    fs.writeFileSync(output, "");
    fs.writeFileSync(summary, "");
    const result = spawnSync("bash", ["-c", authorize], {
      encoding: "utf8",
      timeout: 10_000,
      // Do not pass the test process's credentials or other environment.
      env: {
        PATH: fixture + path.delimiter + process.env.PATH,
        GH_TOKEN: "",
        REPO: repository,
        PR: "123",
        TRIGGER_SHA: triggerSha,
        PERF_GATE_TEST_PR: apiResponse,
        GITHUB_OUTPUT: output,
        GITHUB_STEP_SUMMARY: summary,
      },
    });
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stdout + result.stderr);
    return {
      outputs: Object.fromEntries(
        fs.readFileSync(output, "utf8").trim().split("\n").map((line) => {
          const separator = line.indexOf("=");
          return [line.slice(0, separator), line.slice(separator + 1)];
        }),
      ),
      summary: fs.readFileSync(summary, "utf8"),
    };
  } finally {
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

for (const [name, overrides] of [
  ["ready", {}],
  ["draft", { draft: true }],
  ["bench label", { labels: [{ name: "bench" }] }],
  ["bench title", { title: "[bench] Adopt a cache fix" }],
  ["draft bench label", { draft: true, labels: [{ name: "bench" }] }],
  ["draft bench title", { draft: true, title: "[bench] Adopt a cache fix" }],
]) {
  test(`an adopted contribution stays quarantined when ${name}`, () => {
    const pr = pullRequest(overrides);
    pr.head.ref = "review-contribution/pr-1448-busy-lookup";
    const result = authorization(pr);
    assert.deepEqual(result.outputs, { eligible: "false" });
    assert.match(result.summary, /adopts external code/);
  });
}

test("an ordinary ready same-repository PR remains eligible", () => {
  const result = authorization(pullRequest());
  assert.deepEqual(result.outputs, {
    eligible: "true",
    head_sha: triggerSha,
    base_ref: "main",
    samples: "6",
    context_samples: "1",
    contention_samples: "6",
  });
});

test("an ordinary draft without a bench request stays skipped", () => {
  const result = authorization(pullRequest({ draft: true }));
  assert.deepEqual(result.outputs, { eligible: "false" });
  assert.match(result.summary, /is a draft/);
});

for (const [name, overrides] of [
  ["label", { labels: [{ name: "bench" }] }],
  ["title", { title: "[bench] Measure cache changes" }],
]) {
  test(`an ordinary draft can request benchmarks with its ${name}`, () => {
    const result = authorization(pullRequest({ draft: true, ...overrides }));
    assert.equal(result.outputs.eligible, "true");
    assert.equal(result.outputs.context_samples, "6");
    assert.equal(result.outputs.contention_samples, "6");
  });
}

for (const [name, change, reason] of [
  ["a fork", (pr) => { pr.head.repo.full_name = "contributor/kache"; }, /from a fork/],
  ["a closed PR", (pr) => { pr.state = "closed"; }, /is closed/],
  ["a stale head", (pr) => { pr.head.sha = "newer-head"; }, /newer push supersedes/],
  ["a missing head branch", (pr) => { delete pr.head.ref; }, /no head branch/],
  ["a missing base branch", (pr) => { delete pr.base.ref; }, /no base branch/],
]) {
  test(`${name} cannot authorize a self-hosted measurement`, () => {
    const pr = pullRequest();
    change(pr);
    const result = authorization(pr);
    assert.deepEqual(result.outputs, { eligible: "false" });
    assert.match(result.summary, reason);
  });
}

test("quarantine applies to the branch prefix rather than an interior substring", () => {
  const pr = pullRequest();
  pr.head.ref = "feature/review-contribution/cache-fix";
  assert.equal(authorization(pr).outputs.eligible, "true");
});

for (const [name, overrides] of [
  ["ordinary", {}],
  ["expanded", { labels: [{ name: "bench" }] }],
]) {
  test(`${name} CI can reject sustained cold duplicate growth`, () => {
    const outputs = authorization(pullRequest(overrides)).outputs;
    const measure = workflow.jobs.measure.steps.find(
      (step) => step.name === "Measure repeated samples",
    );
    const coldEvery = measure.run.match(/--contention-cold-every (\d+)/)?.[1];
    assert.ok(coldEvery, "measurement must pass a cold contention cadence");
    assert.equal(measure.env.BENCH_CONTENTION_SAMPLES, "${{ needs.authorize.outputs.contention_samples }}");
    assert.match(measure.run, /--contention-samples "\$BENCH_CONTENTION_SAMPLES"/);
    const result = spawnSync("python3", [
      "-m", "unittest", "bench.tests.test_contention_counts.ContentionCountTests.test_ci_sampling_rejects_sustained_cold_growth",
    ], {
      encoding: "utf8", timeout: 10_000,
      env: {
        PATH: process.env.PATH,
        PYTHONPATH: path.resolve(import.meta.dirname, "../../scripts"),
        PERF_TEST_CONTENTION_SAMPLES: outputs.contention_samples,
        PERF_TEST_COLD_EVERY: coldEvery,
        PERF_TEST_CONTEXT_SAMPLES: outputs.context_samples,
      },
    });
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stdout + result.stderr);
  });
}
