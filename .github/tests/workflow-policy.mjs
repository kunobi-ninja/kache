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
// Job status functions. By default the run is healthy; a check can override
// one, e.g. `{ cancelled: true }` for a run superseded by a newer push.
function statusFunctions(overrides) {
  const values = {
    always: true,
    success: true,
    cancelled: false,
    failure: false,
    ...overrides,
  };
  return new Map(
    Object.entries(values).map(([name, value]) => [
      name,
      {
        name,
        minArgs: 0,
        maxArgs: 0,
        call: () => new data.BooleanData(value),
      },
    ]),
  );
}
let checks = 0;
function evaluate(source, context, status = {}) {
  const custom = statusFunctions(status);
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
  "bench-firefox-windows.yml:bench-firefox-windows",
  "bench-firefox-windows.yml:bench-firefox-pull-windows",
  "perf-gate.yml:measure",
  "prefetch-qualification.yml:seed",
  "prefetch-qualification-consumer.yml:consume",
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
    evaluate(
      files["bench-firefox-windows.yml"].jobs["bench-firefox-windows"].if,
      ctx,
    ),
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
    files["bench-firefox-windows.yml"].jobs["bench-firefox-windows"]["runs-on"],
    canonical,
  ),
  ["self-hosted", "Windows", "X64", "kunobi-windows"],
  "canonical Windows pool retained",
);
// A newer push cancels the in-flight perf-gate run. Its report must not replace
// the pull request's comment with "could not run"; the newer run reports.
// A measurement that fails on its own still gets reported.
{
  const report = files["perf-gate.yml"].jobs.report.if;
  const ctx = context("kunobi-ninja/kache", false);
  ctx.needs = {
    authorize: { outputs: { eligible: "true" } },
    measure: { result: "failure" },
  };
  eq(evaluate(report, ctx), true, "perf report after a failed measurement");
  eq(
    evaluate(report, ctx, { cancelled: true, success: false }),
    false,
    "perf report skipped when a newer run cancelled this one",
  );
  ctx.needs.authorize.outputs.eligible = "false";
  eq(evaluate(report, ctx), false, "perf report skipped when ineligible");
}
// Restoring is safe on PRs, but only a main push may replenish the Nix store.
{
  const cache = files["ci.yml"].jobs["nix-package"].steps.find((step) =>
    step.uses?.startsWith("nix-community/cache-nix-action@"),
  );
  assert.ok(cache, "Nix package jobs restore the Nix store");
  for (const [event, ref, allowed] of [
    ["push", "refs/heads/main", true],
    ["push", "refs/heads/feature", false],
    ["push", "refs/tags/v0.26.0", false],
    ["pull_request", "refs/pull/1/merge", false],
    ["pull_request_target", "refs/heads/main", false],
    ["workflow_dispatch", "refs/heads/main", false],
  ]) {
    const ctx = context("kunobi-ninja/kache", false, {}, event);
    ctx.github.ref = ref;
    eq(evaluate(cache.with.save, ctx), allowed, `Nix cache save: ${event} ${ref}`);
  }
}
{
  const steps = Object.values(files["ci.yml"].jobs).flatMap((job) => job.steps || []);
  assert.ok(
    steps.some((step) => step.uses?.startsWith("zondax/actions/setup-runner@")),
    "CI prepares native tools via setup-runner",
  );
  assert.ok(
    steps.some((step) => step.uses?.startsWith("zondax/actions/setup-mise@")),
    "CI installs toolchains via setup-mise",
  );
  assert.ok(
    !steps.some((step) => step.uses?.startsWith("jdx/mise-action@")),
    "CI does not call jdx/mise-action directly",
  );
  assert.ok(
    !steps.some((step) => /ci-linux-tools|ci-windows-tools/.test(step.run || "")),
    "CI does not keep in-repo runner bootstrap scripts",
  );
}
for (const name of ["ci.yml", "service-image.yml"]) {
  eq(files[name].on.push.branches.includes("dev"), true, `${name}: extra push lane is present`);
}
eq(
  files["service-image.yml"].on.pull_request.branches.includes("dev"),
  true,
  "service image extra pull_request lane is present",
);
for (const [repo, privateRepo] of [
  ["Zondax/kache", true],
  ["kunobi-ninja/kache", false],
]) {
  const ctx = context(repo, privateRepo, privateVars, "push");
  Object.assign(ctx.github, {ref: "refs/heads/dev", ref_name: "dev", ref_type: "branch"});
  eq(
    evaluate(files["service-image.yml"].jobs["docker-dry-run"].if, ctx),
    true,
    `${repo}: extra lane builds the service image without publishing`,
  );
  for (const [workflow, job] of [
    ["service-image.yml", "release"],
    ["ci.yml", "release"],
    ["ci.yml", "publish-chart"],
    ["ci.yml", "stable-branch"],
  ]) {
    eq(
      evaluate(files[workflow].jobs[job].if, ctx),
      false,
      `${repo}: extra lane does not run ${job}`,
    );
  }
}
// Isolated prefetch controls use the benchmark pool only behind canonical-main authorization.
{
  const qualification = files["prefetch-qualification.yml"].jobs;
  const consumer = files["prefetch-qualification-consumer.yml"].jobs.consume;
  for (const repository of publicRepos) {
    for (const ref of ["refs/heads/main", "refs/heads/topic", "refs/pull/1/merge"]) {
      const ctx = context(repository, false, {}, "workflow_dispatch");
      ctx.github.ref = ref;
      const allowed = repository === "kunobi-ninja/kache" && ref === "refs/heads/main";
      eq(evaluate(qualification.authorize.if, ctx), allowed, "qualification authorization");
      eq(evaluate(consumer.if, ctx), allowed, "qualification consumer guard");
    }
  }
  eq(qualification.seed.needs, "authorize", "seed depends on trusted authorization");
  for (const event of ["push", "workflow_dispatch", "pull_request"]) {
    for (const protectedRef of [true, false]) {
      const ctx = context("kunobi-ninja/kache", false, {}, event);
      ctx.github.ref_protected = protectedRef;
      eq(evaluate(qualification.seed.if, ctx), event === "push" && protectedRef,
        "seed requires protected push");
    }
  }
  for (const job of [qualification.seed, consumer]) {
    eq(resolveRunner(job["runs-on"], context("kunobi-ninja/kache", false)),
      "kunobi-runners", "qualification uses existing measurement pool");
    eq(resolveRunner(job["runs-on"], context("kunobi-ninja/kache", false,
      { BENCH_RUNNER_LINUX: '\"benchmark-override\"' })), "benchmark-override",
      "qualification respects measurement runner override");
  }
  const order = ["off-1", "on-1", "on-2", "off-2", "off-3", "on-3"];
  for (const [index, arm] of order.entries()) {
    eq(qualification[arm].needs, ["authorize", index ? order[index - 1] : "seed"],
      `${arm} follows explicit consumer order`);
    eq(qualification[arm].with.arm, arm, `${arm} selects its declared setting`);
  }
  for (const [event, authorized, seed, allowed] of [
    ["push", "success", "success", true],
    ["push", "success", "skipped", false],
    ["workflow_dispatch", "success", "skipped", true],
    ["workflow_dispatch", "failure", "skipped", false],
    ["push", "success", "failure", false],
  ]) {
    const ctx = context("kunobi-ninja/kache", false, {}, event);
    ctx.needs = { authorize: { result: authorized }, seed: { result: seed } };
    eq(evaluate(qualification["off-1"].if, ctx), allowed, "first consumer seed admission");
  }
  const ctx = context("kunobi-ninja/kache", false);
  ctx.needs.authorize = { result: "success" };
  eq(evaluate(qualification.collect.if, ctx, { success: false }), true,
    "collector retains failed measurement timings");
}
console.log(
  `${checks} workflow policy checks passed across ${routing.length} validation selectors.`,
);
