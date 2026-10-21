#!/usr/bin/env node
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import { parse, stringify } from "yaml";

const root = path.dirname(fileURLToPath(import.meta.url));
const schemaHashes = {
  installer: "47de5aefdea4e7ccbc8ab4e32e8230671300054bd4f688cd7ca8bdebf459f006",
  version: "0cdf9c17a0d19221a3612980c95a47d120c4ce532157177aa6af4368a8a78273",
  defaultLocale:
    "b87eaa2252daf0bc9b2d495a3f7f5547b8b36236bdcc0543cf9f7403bd56707e",
  locale: "f049048815dc30599722f0419545c6dbfc8e51c2fb52ff4ac113a60069032ab7",
};

function validators() {
  const ajv = new Ajv({ allErrors: true, strict: false });
  addFormats(ajv);
  ajv.addFormat("long", { type: "number", validate: Number.isSafeInteger });
  return Object.fromEntries(
    Object.entries(schemaHashes).map(([type, expected]) => {
      const bytes = fs.readFileSync(
        path.join(root, "schemas", `manifest.${type}.1.12.0.json`),
      );
      assert.equal(
        createHash("sha256").update(bytes).digest("hex"),
        expected,
        `${type} schema changed`,
      );
      return [type, ajv.compile(JSON.parse(bytes))];
    }),
  );
}

function yamlFiles(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const file = path.join(directory, entry.name);
    assert(!entry.isSymbolicLink(), `unexpected manifest symlink: ${file}`);
    return entry.isDirectory()
      ? yamlFiles(file)
      : entry.name.endsWith(".yaml")
        ? [file]
        : [];
  });
}

export function prepare({
  directory,
  archives,
  baseInstaller,
  packageId,
  version,
}) {
  assert(
    ["kunobi-ninja.kache", "kunobi-ninja.kache.Unstable"].includes(packageId),
    "unexpected package identifier",
  );
  const inspection = JSON.parse(
    execFileSync(
      "python3",
      [path.join(root, "inspect_archives.py"), archives, "--version", version],
      { encoding: "utf8" },
    ),
  );
  const files = yamlFiles(directory);
  const manifests = files.map((file) => ({
    file,
    data: parse(fs.readFileSync(file, "utf8")),
  }));
  assert.equal(
    manifests.filter(({ data }) => data.ManifestType === "installer").length,
    1,
    "expected one installer manifest",
  );
  assert.equal(
    manifests.filter(({ data }) => data.ManifestType === "version").length,
    1,
    "expected one version manifest",
  );
  assert.equal(
    manifests.filter(({ data }) => data.ManifestType === "defaultLocale")
      .length,
    1,
    "expected one default locale manifest",
  );
  for (const { data } of manifests) {
    assert.equal(
      data.PackageIdentifier,
      packageId,
      "manifest package identifier differs",
    );
    assert.equal(
      data.PackageVersion,
      version,
      "manifest package version differs",
    );
    assert.equal(
      data.ManifestVersion,
      "1.12.0",
      "unsupported Winget schema version",
    );
  }
  const installer = manifests.find(
    ({ data }) => data.ManifestType === "installer",
  );
  const base = parse(fs.readFileSync(baseInstaller, "utf8"));
  assert.equal(
    base.PackageIdentifier,
    packageId,
    "base installer package identifier differs",
  );
  assert.equal(
    base.ManifestVersion,
    "1.12.0",
    "unsupported base installer schema",
  );
  assert.equal(
    base.Installers?.length,
    2,
    "base manifest must contain both Windows installers",
  );
  assert(
    installer.data.Installers?.length >= 1 &&
      installer.data.Installers.length <= 2,
    "unexpected generated installer count",
  );
  const seen = new Set();
  for (const row of installer.data.Installers) {
    const inspected = inspection.installers.find(
      ({ url }) => url === row.InstallerUrl,
    );
    assert(inspected, `unexpected installer URL: ${row.InstallerUrl}`);
    assert(
      !seen.has(inspected.architecture),
      "duplicate installer architecture",
    );
    seen.add(inspected.architecture);
    assert.equal(
      row.InstallerSha256?.toLowerCase(),
      inspected.sha256,
      "installer checksum differs from release asset",
    );
    assert.deepEqual(
      row.Commands ?? installer.data.Commands,
      ["kache"],
      "unexpected command aliases",
    );
  }
  // Komac can collapse two multi-executable ZIPs into one installer. Preserve
  // the published architecture-specific metadata and bind it to inspected assets.
  const prepared = structuredClone(base);
  prepared.PackageVersion = version;
  delete prepared.ReleaseDate;
  if (installer.data.ReleaseDate)
    prepared.ReleaseDate = installer.data.ReleaseDate;
  prepared.Installers = inspection.installers.map((inspected) => {
    const candidates = base.Installers.filter(
      (row) =>
        (row.Architecture ?? base.Architecture) === inspected.architecture,
    );
    assert.equal(
      candidates.length,
      1,
      `base manifest is missing or duplicates ${inspected.architecture}`,
    );
    const row = structuredClone(candidates[0]);
    assert.deepEqual(
      row.Commands ?? base.Commands,
      ["kache"],
      "unexpected base command aliases",
    );
    row.Architecture = inspected.architecture;
    row.InstallerUrl = inspected.url;
    row.InstallerSha256 = inspected.sha256.toUpperCase();
    delete row.InstallerType;
    delete row.NestedInstallerType;
    delete row.NestedInstallerFiles;
    delete row.SignatureSha256;
    delete row.ReleaseDate;
    return row;
  });
  installer.data = prepared;
  delete installer.data.Architecture;
  installer.data.InstallerType =
    inspection.layout === "paired" ? "zip" : "portable";
  if (inspection.layout === "paired") {
    installer.data.NestedInstallerType = "portable";
    // Winget retains the whole archive; expose only the frontend command.
    installer.data.NestedInstallerFiles = [
      { RelativeFilePath: "kache.exe", PortableCommandAlias: "kache" },
    ];
  } else {
    delete installer.data.NestedInstallerType;
    delete installer.data.NestedInstallerFiles;
  }
  const validate = validators();
  for (const { file, data } of manifests) {
    const check = validate[data.ManifestType];
    assert(check, `unexpected manifest type: ${data.ManifestType}`);
    assert(check(data), `${file}: ${JSON.stringify(check.errors)}`);
  }
  const versionManifest = manifests.find(
    ({ data }) => data.ManifestType === "version",
  ).data;
  const defaultLocale = manifests.find(
    ({ data }) => data.ManifestType === "defaultLocale",
  ).data;
  assert.equal(
    versionManifest.DefaultLocale,
    defaultLocale.PackageLocale,
    "default locale differs between manifests",
  );
  // Validate the complete set before changing any generated file.
  const header = `# yaml-language-server: $schema=https://aka.ms/winget-manifest.installer.1.12.0.schema.json\n\n`;
  fs.writeFileSync(installer.file, header + stringify(installer.data));
  return {
    layout: inspection.layout,
    files: manifests.length,
    installer: installer.file,
  };
}

if (
  process.argv[1] &&
  pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url
) {
  const { values } = parseArgs({
    options: {
      directory: { type: "string" },
      archives: { type: "string" },
      "base-installer": { type: "string" },
      "package-id": { type: "string" },
      version: { type: "string" },
    },
  });
  assert(
    values.directory &&
      values.archives &&
      values["base-installer"] &&
      values["package-id"] &&
      values.version,
    "directory, archives, base-installer, package-id and version are required",
  );
  console.log(
    JSON.stringify(
      prepare({
        directory: values.directory,
        archives: values.archives,
        baseInstaller: values["base-installer"],
        packageId: values["package-id"],
        version: values.version,
      }),
    ),
  );
}
