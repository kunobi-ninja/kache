# Windows package manifests

Run `npm ci --ignore-scripts` and `npm test` here with Node 24 and Python 3.

The publisher downloads both Windows ZIPs and their checksum files. Run
`python3 inspect_archives.py <assets> --version <version> --select` to select
the installer URLs. Paired archives use the ZIP; backfills of monolithic
releases use their bare EXEs, which must match the ZIP payloads.

Save the published installer with `komac show <id> --installer-manifest > <base>`.
Generate manifests with `komac update --dry-run --output <manifests>`, then run:

```sh
node prepare.mjs --directory <manifests> --archives <assets> \
  --base-installer <base> --package-id kunobi-ninja.kache --version <version>
```

Preparation checks archive contents, PE architectures, checksums and the whole
manifest set before updating the installer file. Submit the resulting directory
with `komac submit --yes <manifests>`.

The published installer supplies metadata for both architectures. Komac can
drop an architecture when analysing ZIPs with more than one executable.

The schema files are unmodified Microsoft Winget 1.12 schemas from
[`microsoft/winget-cli` at `3d9083c0`](https://github.com/microsoft/winget-cli/tree/3d9083c062c1e7a7086e05becb414d4e005e4ab6/schemas/JSON/manifests/v1.12.0).
Their MIT license is in `schemas/LICENSE`. Their hashes and the schema version
are checked by `prepare.mjs`; update them together when upgrading Komac.
