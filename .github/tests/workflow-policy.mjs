import assert from "node:assert/strict";
import fs from "node:fs";
import { parse } from "yaml";
import { Lexer, Parser, Evaluator, data } from "@actions/expressions";

const root = process.argv[2];
const files = Object.fromEntries(
  fs
    .readdirSync(`${root}/.github/workflows`)
    .filter((n) => /\.ya?ml$/.test(n))
    .map((n) => [
      n,
      parse(fs.readFileSync(`${root}/.github/workflows/${n}`, "utf8")),
    ]),
);
const custom = new Map(
  ["always", "success"].map((name) => [
    name,
    {
      name,
      minArgs: 0,
      maxArgs: 0,
      call: () => new data.BooleanData(true),
    },
  ]),
);
let checks = 0;
function evaluate(source, context) {
  const expression = source
    .trim()
    .replace(/^\$\{\{\s*/, "")
    .replace(/\s*\}\}$/, "");
  const tokens = new Lexer(expression).lex().tokens;
  const ast = new Parser(tokens, Object.keys(context), [
    ...custom.values(),
  ]).parse();
  const value = new Evaluator(
    ast,
    JSON.parse(JSON.stringify(context), data.reviver),
    custom,
  ).evaluate();
  return JSON.parse(JSON.stringify(value, data.replacer));
}
function eq(actual, expected, label) {
  checks++;
  assert.deepEqual(actual, expected, label);
}
function context(repository, privateRepo, vars = {}, event = "pull_request") {
  return {
    github: {
      repository,
      event_name: event,
      ref: "refs/tags/v0.16.1",
      ref_name: "v0.16.1",
      ref_type: "tag",
      event: {
        repository: {
          private: privateRepo,
          fork: repository === "contributor/example",
        },
        pull_request: {
          head: { repo: { full_name: repository } },
          draft: false,
        },
        workflow_run: { conclusion: "success", event: "pull_request" },
        inputs: { scenario: "all" },
      },
    },
    vars,
    matrix: {},
    needs: {},
    inputs: { only: "all" },
  };
}
const privateVars = {
  CI_RUNNER_LINUX: '"private-linux"',
  CI_RUNNER_MACOS: '["self-hosted","macOS","ARM64","ci"]',
  CI_RUNNER_WINDOWS: '["self-hosted","Windows","X64","ci"]',
};
const publicRepos = [
  "kunobi-ninja/kache",
  "Zondax/example",
  "contributor/example",
];
const privateRepos = [
  "Zondax/example",
  "kunobi-ninja/example",
  "contributor/example",
];
// Publication-only workflows and measurement pools have separate checks below.
// Discover every other runner, even if its expression loses CI_RUNNER_*.
const publicationWorkflows = new Set([
  "package-publish.yml",
  "publish-crates.yaml",
]);
const measurementJobs = new Set([
  "bench.yml:bench",
  "bench.yml:bench-firefox-windows",
  "bench.yml:bench-firefox-pull-windows",
  "perf-gate.yml:measure",
]);
const routing = [];
for (const [file, workflow] of Object.entries(files)) {
  if (publicationWorkflows.has(file)) continue;
  for (const [id, job] of Object.entries(workflow.jobs)) {
    const name = `${file}:${id}`;
    if (measurementJobs.has(name) || job.uses) continue;
    assert(Object.hasOwn(job, "runs-on"), `${name} must select a runner`);
    if (/^\$\{\{\s*matrix\.runner\s*\}\}$/.test(job["runs-on"])) {
      const rows = job.strategy?.matrix?.include;
      assert(rows?.length, `${name} must declare its runner matrix`);
      for (const row of rows) {
        assert(
          Object.hasOwn(row, "runner"),
          `${name}:${row.os} needs a runner`,
        );
        routing.push([`${name}:${row.os}`, row.runner]);
      }
    } else {
      routing.push([name, job["runs-on"]]);
    }
  }
}
assert(routing.length > 0);
function resolveRunner(source, context) {
  return typeof source === "string" && source.trim().startsWith("${{")
    ? evaluate(source, context)
    : source;
}
for (const [name, expression] of routing) {
  const os = /:(cargo-macos|macOS)$/.test(name)
    ? "MACOS"
    : /:(cargo-windows|Windows)$/.test(name)
      ? "WINDOWS"
      : "LINUX";
  const platform = `CI_RUNNER_${os}`;
  const hosted =
    name === "ci.yml:kani"
      ? "ubuntu-24.04"
      : {
          LINUX: "ubuntu-latest",
          MACOS: "macos-latest",
          WINDOWS: "windows-latest",
        }[os];
  for (const repository of publicRepos) {
    eq(
      resolveRunner(expression, context(repository, false)),
      hosted,
      `${name} public default`,
    );
    eq(
      resolveRunner(expression, context(repository, false, privateVars)),
      hosted,
      `${name} ignores private overrides in public`,
    );
  }
  const forkPr = context("kunobi-ninja/kache", false, privateVars);
  forkPr.github.event.pull_request.head.repo.full_name = "contributor/example";
  eq(
    resolveRunner(expression, forkPr),
    hosted,
    `${name} external fork PR remains hosted`,
  );
  for (const repository of privateRepos) {
    eq(
      resolveRunner(expression, context(repository, true, privateVars)),
      JSON.parse(privateVars[platform]),
      `${name} private configured`,
    );
    checks++;
    assert.throws(
      () => resolveRunner(expression, context(repository, true)),
      undefined,
      `${name} missing private selector`,
    );
    checks++;
    assert.throws(
      () =>
        resolveRunner(
          expression,
          context(repository, true, {
            ...privateVars,
            [platform]: "broken JSON",
          }),
        ),
      undefined,
      `${name} malformed selector`,
    );
  }
}

const publication = [
  ["ci.yml", "release"],
  ["ci.yml", "stable-branch"],
  ["ci.yml", "publish-chart"],
  ["publish-crates.yaml", "publish"],
  ["package-publish.yml", "resolve"],
  ["service-image.yml", "release"],
];
for (const [file, id] of publication) {
  for (const repository of [...publicRepos, ...privateRepos]) {
    eq(
      evaluate(
        files[file].jobs[id].if,
        context(repository, repository !== "kunobi-ninja/kache", {}, "push"),
      ),
      repository === "kunobi-ninja/kache",
      `${file}:${id} canonical publication`,
    );
  }
}
const packageJobs = files["package-publish.yml"].jobs;
function dependsOnResolve(id, seen = new Set()) {
  if (id === "resolve") return true;
  if (seen.has(id)) return false;
  seen.add(id);
  const job = packageJobs[id];
  assert(!job.if || !/always\(|cancelled\(|failure\(/.test(job.if));
  return [job.needs]
    .flat()
    .some((parent) => dependsOnResolve(parent, new Set(seen)));
}
for (const id of Object.keys(packageJobs))
  eq(
    dependsOnResolve(id),
    true,
    `package publication ${id} depends on guarded resolve`,
  );

for (const repo of ["Zondax/example", "contributor/example"]) {
  const ctx = context(repo, false);
  eq(
    evaluate(files["bench.yml"].jobs.plan.if, ctx),
    false,
    "copy benchmark disabled",
  );
  eq(
    evaluate(files["bench.yml"].jobs["bench-firefox-windows"].if, ctx),
    false,
    "copy Windows benchmark disabled",
  );
  eq(
    evaluate(files["perf-gate-preflight.yml"].jobs.preflight.if, ctx),
    false,
    "copy perf preflight disabled",
  );
  eq(
    evaluate(files["perf-gate.yml"].jobs.authorize.if, ctx),
    false,
    "copy perf authorization disabled",
  );
  const schedule = context(repo, false, {}, "schedule");
  eq(
    evaluate(files["fuzz.yml"].jobs["native-archive"].if, schedule),
    false,
    "copy scheduled fuzz disabled",
  );
  eq(
    evaluate(files["fuzz.yml"].jobs["native-archive"].if, ctx),
    true,
    "copy PR fuzz enabled",
  );
  const push = context(repo, false, {}, "push");
  eq(
    evaluate(files["service-image.yml"].jobs["docker-dry-run"].if, push),
    true,
    "copy image builds without publish",
  );
}
const benchmarkVars = {
  ...privateVars,
  ENABLE_BENCHMARKS: "true",
  ENABLE_PERF_GATE: "true",
  ENABLE_SCHEDULED_JOBS: "true",
  PERF_RUNNER_LINUX: '"private-perf"',
  BENCH_RUNNER_LINUX: '"private-bench"',
  BENCH_RUNNER_LINUX_LARGE: '"private-large"',
  BENCH_RUNNER_WINDOWS: '["self-hosted","Windows","bench"]',
};
for (const privateRepo of [false, true]) {
  const ctx = context("Zondax/example", privateRepo, benchmarkVars, "schedule");
  eq(evaluate(files["bench.yml"].jobs.plan.if, ctx), true, "benchmark opt-in");
  eq(
    evaluate(files["fuzz.yml"].jobs["native-archive"].if, ctx),
    true,
    "fuzz opt-in",
  );
  eq(
    evaluate(files["perf-gate-preflight.yml"].jobs.preflight.if, ctx),
    true,
    "perf opt-in",
  );
  eq(
    evaluate(files["perf-gate.yml"].jobs.measure["runs-on"], ctx),
    "private-perf",
    "perf configured pool",
  );
  for (const [pool, expected] of [
    ["kunobi-runners", "private-bench"],
    ["kunobi-runners-large", "private-large"],
  ]) {
    ctx.matrix = { runner: pool };
    eq(
      evaluate(files["bench.yml"].jobs.bench["runs-on"], ctx),
      expected,
      "benchmark configured pool",
    );
  }
  delete ctx.vars.BENCH_RUNNER_LINUX_LARGE;
  eq(
    evaluate(files["bench.yml"].jobs.bench["runs-on"], ctx),
    null,
    "missing large pool does not use small pool",
  );
  ctx.vars.BENCH_RUNNER_LINUX_LARGE = '"private-large"';
}
const canonical = context("kunobi-ninja/kache", false, {}, "schedule");
for (const pool of ["kunobi-runners", "kunobi-runners-large"]) {
  canonical.matrix = { runner: pool };
  eq(
    evaluate(files["bench.yml"].jobs.bench["runs-on"], canonical),
    pool,
    "canonical benchmark pool retained",
  );
}
eq(
  evaluate(files["perf-gate.yml"].jobs.measure["runs-on"], canonical),
  "kunobi-runners",
  "canonical perf pool retained",
);
eq(
  evaluate(
    files["bench.yml"].jobs["bench-firefox-windows"]["runs-on"],
    canonical,
  ),
  ["self-hosted", "Windows", "X64", "kunobi-windows"],
  "canonical Windows pool retained",
);
console.log(
  `${checks} workflow policy checks passed across ${routing.length} validation selectors.`,
);
