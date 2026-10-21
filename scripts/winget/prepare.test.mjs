import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { parse, stringify } from "yaml";
import { prepare } from "./prepare.mjs";

const source = path.dirname(fileURLToPath(import.meta.url));

function fixture(t, { legacy = false, unstable = false } = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "kache-winget-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const archives = path.join(root, "archives");
  execFileSync("python3", [
    path.join(source, "test_inspect_archives.py"),
    "fixture",
    archives,
    legacy ? "legacy" : "paired",
  ]);
  const version = unstable ? "1.2.3-rc.1" : "1.2.3";
  const packageId = unstable
    ? "kunobi-ninja.kache.Unstable"
    : "kunobi-ninja.kache";
  const inspection = JSON.parse(
    execFileSync(
      "python3",
      [
        path.join(source, "inspect_archives.py"),
        archives,
        "--version",
        version,
      ],
      { encoding: "utf8" },
    ),
  );
  const directory = path.join(root, "manifests", packageId, version);
  fs.mkdirSync(directory, { recursive: true });
  const common = {
    PackageIdentifier: packageId,
    PackageVersion: version,
    ManifestVersion: "1.12.0",
  };
  const data = {
    installer: {
      ...common,
      ManifestType: "installer",
      InstallerType: "portable",
      MinimumOSVersion: "10.0.17763.0",
      Commands: ["kache"],
      Installers: inspection.installers.map((row) => ({
        Architecture: row.architecture,
        InstallerUrl: row.url,
        InstallerSha256: row.sha256.toUpperCase(),
      })),
    },
    version: { ...common, ManifestType: "version", DefaultLocale: "en-US" },
    defaultLocale: {
      ...common,
      ManifestType: "defaultLocale",
      PackageLocale: "en-US",
      Publisher: "Kunobi",
      PackageName: "Kache",
      License: "Apache-2.0",
      ShortDescription: "Compiler cache",
      ReleaseNotesUrl: `https://github.com/kunobi-ninja/kache/releases/tag/v${version}`,
    },
  };
  const files = Object.fromEntries(
    Object.keys(data).map((type) => [
      type,
      path.join(directory, `${packageId}.${type}.yaml`),
    ]),
  );
  const baseInstaller = path.join(root, "base-installer.yaml");
  const base = structuredClone(data.installer);
  base.PackageVersion = "1.2.2";
  const writeBase = () => fs.writeFileSync(baseInstaller, stringify(base));
  const write = () => {
    for (const [type, file] of Object.entries(files))
      fs.writeFileSync(file, stringify(data[type]));
  };
  write();
  writeBase();
  return {
    args: { directory, archives, baseInstaller, packageId, version },
    data,
    base,
    writeBase,
    files,
    write,
    read: () => parse(fs.readFileSync(files.installer, "utf8")),
  };
}

for (const unstable of [false, true]) {
  test(`paired ${unstable ? "unstable" : "stable"} package installs the archive and keeps the frontend alias`, (t) => {
    const f = fixture(t, { unstable });
    const locale = fs.readFileSync(f.files.defaultLocale);
    const version = fs.readFileSync(f.files.version);
    const result = prepare(f.args);
    assert.equal(result.layout, "paired");
    const installer = f.read();
    assert.equal(installer.InstallerType, "zip");
    assert.equal(installer.NestedInstallerType, "portable");
    assert.deepEqual(installer.NestedInstallerFiles, [
      { RelativeFilePath: "kache.exe", PortableCommandAlias: "kache" },
    ]);
    assert.deepEqual(installer.Commands, ["kache"]);
    assert.deepEqual(
      installer.Installers.map((row) => row.Architecture),
      ["x64", "arm64"],
    );
    assert(
      installer.Installers.every((row) => row.InstallerUrl.endsWith(".zip")),
    );
    assert.deepEqual(fs.readFileSync(f.files.defaultLocale), locale);
    assert.deepEqual(fs.readFileSync(f.files.version), version);
    assert.deepEqual(prepare(f.args), result, "preparation is repeatable");
  });
}

test("per-installer types and guessed archive architectures are replaced with inspected values", (t) => {
  const f = fixture(t);
  f.base.Scope = "user";
  f.base.Installers[0].MinimumOSVersion = "10.0.19041.0";
  f.base.Installers[1].MinimumOSVersion = "10.0.22000.0";
  f.writeBase();
  f.data.installer.Installers.forEach((row) => {
    row.Architecture = "x86";
    row.InstallerType = "portable";
    row.NestedInstallerType = "exe";
    row.NestedInstallerFiles = [{ RelativeFilePath: "old.exe" }];
  });
  f.write();
  prepare(f.args);
  const installer = f.read();
  assert.equal(installer.Scope, "user");
  assert.deepEqual(
    installer.Installers.map((row) => row.Architecture),
    ["x64", "arm64"],
  );
  assert.deepEqual(
    installer.Installers.map((row) => row.MinimumOSVersion),
    ["10.0.19041.0", "10.0.22000.0"],
  );
  for (const row of installer.Installers) {
    assert(!Object.hasOwn(row, "InstallerType"));
    assert(!Object.hasOwn(row, "NestedInstallerType"));
    assert(!Object.hasOwn(row, "NestedInstallerFiles"));
  }
});

test("monolithic release backfills retain portable EXE installers", (t) => {
  const f = fixture(t, { legacy: true });
  f.data.installer.NestedInstallerType = "portable";
  f.data.installer.NestedInstallerFiles = [{ RelativeFilePath: "kache.exe" }];
  f.write();
  assert.equal(prepare(f.args).layout, "legacy");
  const installer = f.read();
  assert.equal(installer.InstallerType, "portable");
  assert(!Object.hasOwn(installer, "NestedInstallerFiles"));
  assert(!Object.hasOwn(installer, "NestedInstallerType"));
  assert(
    installer.Installers.every((row) => row.InstallerUrl.endsWith(".exe")),
  );
});

test("a real multi-executable ZIP analysis that dropped x64 is repaired from published metadata", (t) => {
  const f = fixture(t);
  f.data.installer.Installers.shift();
  f.data.installer.ReleaseDate = "2026-10-20";
  f.base.ReleaseDate = "2026-10-15";
  f.base.Installers[0].Scope = "user";
  f.base.Installers[1].Scope = "machine";
  f.writeBase();
  f.write();
  prepare(f.args);
  const installer = f.read();
  assert.deepEqual(
    installer.Installers.map((row) => row.Architecture),
    ["x64", "arm64"],
  );
  assert.deepEqual(
    installer.Installers.map((row) => row.Scope),
    ["user", "machine"],
  );
  assert.equal(installer.ReleaseDate, "2026-10-20");
});

for (const [name, mutate, message] of [
  [
    "wrong package",
    (f) => {
      f.data.version.PackageIdentifier = "other.kache";
    },
    /identifier differs/,
  ],
  [
    "wrong version",
    (f) => {
      f.data.defaultLocale.PackageVersion = "9.0.0";
    },
    /version differs/,
  ],
  [
    "wrong checksum",
    (f) => {
      f.data.installer.Installers[0].InstallerSha256 = "0".repeat(64);
    },
    /checksum differs/,
  ],
  [
    "wrong URL",
    (f) => {
      f.data.installer.Installers[0].InstallerUrl =
        "https://example.com/kache.zip";
    },
    /unexpected installer URL/,
  ],
  [
    "duplicate architecture",
    (f) => {
      f.data.installer.Installers[1] = { ...f.data.installer.Installers[0] };
    },
    /duplicate installer architecture/,
  ],
  [
    "changed alias",
    (f) => {
      f.data.installer.Commands = ["different"];
    },
    /command aliases/,
  ],
  [
    "unknown schema",
    (f) => {
      f.data.version.ManifestVersion = "1.28.0";
    },
    /unsupported Winget schema/,
  ],
  [
    "schema-invalid metadata",
    (f) => {
      delete f.data.defaultLocale.Publisher;
    },
    /required property 'Publisher'/,
  ],
  [
    "schema-invalid installer",
    (f) => {
      f.base.Scope = "invalid";
      f.writeBase();
    },
    /enum/,
  ],
  [
    "schema-invalid return code",
    (f) => {
      f.base.InstallerSuccessCodes = [4294967296];
      f.writeBase();
    },
    /maximum/,
  ],
  [
    "missing base architecture",
    (f) => {
      f.base.Installers.pop();
      f.writeBase();
    },
    /both Windows installers/,
  ],
  [
    "duplicate base architecture",
    (f) => {
      f.base.Installers[1].Architecture = "x64";
      f.writeBase();
    },
    /missing or duplicates/,
  ],
  [
    "inconsistent locale",
    (f) => {
      f.data.version.DefaultLocale = "es-ES";
    },
    /default locale differs/,
  ],
]) {
  test(`${name} stops before any generated manifest changes`, (t) => {
    const f = fixture(t);
    mutate(f);
    f.write();
    const before = Object.values(f.files).map((file) => fs.readFileSync(file));
    assert.throws(() => prepare(f.args), message);
    Object.values(f.files).forEach((file, i) =>
      assert.deepEqual(fs.readFileSync(file), before[i]),
    );
  });
}

test("incomplete and duplicate manifest sets are rejected", (t) => {
  const f = fixture(t);
  fs.copyFileSync(
    f.files.installer,
    path.join(f.args.directory, "duplicate.yaml"),
  );
  assert.throws(() => prepare(f.args), /one installer manifest/);
  fs.rmSync(path.join(f.args.directory, "duplicate.yaml"));
  fs.rmSync(f.files.version);
  assert.throws(() => prepare(f.args), /one version manifest/);
});

test("CLI prepares a complete generated manifest set", (t) => {
  const f = fixture(t);
  const result = spawnSync(
    process.execPath,
    [
      path.join(source, "prepare.mjs"),
      "--directory",
      f.args.directory,
      "--archives",
      f.args.archives,
      "--base-installer",
      f.args.baseInstaller,
      "--package-id",
      f.args.packageId,
      "--version",
      f.args.version,
    ],
    { encoding: "utf8", timeout: 10_000 },
  );
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(JSON.parse(result.stdout).layout, "paired");
  assert.equal(f.read().InstallerType, "zip");
});

for (const invalid of [false, true]) {
  test(
    `workflow generation ${invalid ? "rejects a wrong hash" : "validates the paired payload before submission"}`,
    { skip: process.platform === "win32" },
    (t) => {
      const f = fixture(t);
      if (invalid) {
        f.data.installer.Installers[0].InstallerSha256 = "0".repeat(64);
        f.write();
      }
      const repository = path.resolve(source, "../..");
      const workflow = parse(
        fs.readFileSync(
          path.join(repository, ".github/workflows/package-publish.yml"),
          "utf8",
        ),
      );
      const steps = workflow.jobs.winget.steps;
      const generation = steps.findIndex(
        (step) => step.name === "Generate and validate Winget manifests",
      );
      const submission = steps.findIndex(
        (step) => step.name === "Submit validated Winget manifests",
      );
      assert(generation >= 0 && generation < submission);
      assert.equal(steps[generation].env.GITHUB_TOKEN, "${{ secrets.WINGET_TOKEN }}");
      assert.equal(
        steps[submission].env.GITHUB_TOKEN,
        "${{ secrets.WINGET_TOKEN }}",
      );
      const scratch = path.dirname(f.args.archives);
      const bin = path.join(scratch, "bin");
      fs.mkdirSync(bin);
      fs.mkdirSync(path.join(scratch, "winget-assets"));
      fs.cpSync(f.args.archives, path.join(scratch, "winget-assets"), {
        recursive: true,
      });
      fs.writeFileSync(
        path.join(bin, "komac"),
        `#!/usr/bin/env node
const fs = require('node:fs');
const args = process.argv.slice(2);
if (args[0] === 'show') { process.stdout.write(fs.readFileSync(process.env.KACHE_TEST_BASE_INSTALLER)); process.exit(0); }
if (args[0] !== 'update' || !args.includes('--dry-run') || args.includes('--submit')) process.exit(9);
fs.writeFileSync(process.env.KACHE_TEST_KOMAC_ARGS, JSON.stringify(args));
fs.cpSync(process.env.KACHE_TEST_MANIFESTS, args[args.indexOf('--output') + 1], {recursive: true});
`,
        { mode: 0o755 },
      );
      const log = path.join(scratch, "komac-args.json");
      const result = spawnSync(
        "bash",
        ["-e", "-o", "pipefail", "-c", steps[generation].run],
        {
          cwd: repository,
          env: {
            ...process.env,
            PATH: `${bin}:${path.dirname(process.execPath)}:${process.env.PATH}`,
            RUNNER_TEMP: scratch,
            VERSION: f.args.version,
            PACKAGE_ID: f.args.packageId,
            GH_OWNER: "kunobi-ninja",
            GH_REPO: "kache",
            URL_X64: f.data.installer.Installers[0].InstallerUrl,
            URL_ARM: f.data.installer.Installers[1].InstallerUrl,
            RELEASE_NOTES_URL: "",
            KACHE_TEST_KOMAC_ARGS: log,
            KACHE_TEST_MANIFESTS: f.args.directory,
            KACHE_TEST_BASE_INSTALLER: f.args.baseInstaller,
          },
          encoding: "utf8",
          timeout: 10_000,
        },
      );
      assert.ifError(result.error);
      assert.equal(result.status, invalid ? 1 : 0, result.stderr);
      const args = JSON.parse(fs.readFileSync(log, "utf8"));
      assert(
        args.includes(
          `https://github.com/kunobi-ninja/kache/releases/tag/v${f.args.version}`,
        ),
      );
      const installer = parse(
        fs.readFileSync(
          path.join(
            scratch,
            "winget-manifests",
            path.basename(f.files.installer),
          ),
          "utf8",
        ),
      );
      assert.equal(installer.InstallerType, invalid ? "portable" : "zip");
      if (invalid) assert.match(result.stderr, /checksum differs/);
    },
  );
}
