#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

python3 - <<'PY'
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

root = Path.cwd()
docs = root / "docs"
errors: list[str] = []

pages = sorted(docs.rglob("*.mdx"))
for page in pages:
    text = page.read_text()
    if not text.startswith("---\n") or "\n---\n" not in text[4:]:
        errors.append(f"{page}: missing MDX front matter")

for data_file in sorted(docs.rglob("*.json")):
    try:
        json.loads(data_file.read_text())
    except json.JSONDecodeError as exc:
        errors.append(f"{data_file}: invalid JSON: {exc}")

for meta_file in sorted(docs.rglob("meta.json")):
    meta = json.loads(meta_file.read_text())
    for entry in meta.get("pages", []):
        if not isinstance(entry, str):
            errors.append(f"{meta_file}: non-string page entry {entry!r}")
            continue
        candidates = [meta_file.parent / f"{entry}.mdx", meta_file.parent / entry / "meta.json"]
        if not any(candidate.is_file() for candidate in candidates):
            errors.append(f"{meta_file}: missing page or section {entry!r}")

link_re = re.compile(r"(?:\]\(|href=\")(/docs(?:/[^)#?\"]*)?)(?:#[^)\"]+)?(?:\)|\")")
for page in pages:
    text = page.read_text()
    for match in link_re.finditer(text):
        route = match.group(1).removeprefix("/docs").strip("/")
        candidates = [docs / "index.mdx"] if not route else [docs / f"{route}.mdx", docs / route / "index.mdx"]
        if not any(candidate.is_file() for candidate in candidates):
            errors.append(f"{page}: broken internal route {match.group(1)}")

all_prose = "\n".join([root.joinpath("README.md").read_text(), *(p.read_text() for p in pages)])
for obsolete in ["](/getting-started", "](/daemon", "](/remote-cache", "kache status", "install-shims --dir"]:
    if obsolete in all_prose:
        errors.append(f"obsolete documentation spelling remains: {obsolete!r}")

for page in pages:
    if "/docs/kache/" in page.read_text():
        errors.append(f"{page}: upstream links must use /docs/...; kunobi-web adds the /kache product slug")

main = root.joinpath("src/main.rs").read_text()
command_block = main.split("enum Commands {", 1)[1].split("enum DaemonCommands", 1)[0]
variants = re.findall(r"^    ([A-Z][A-Za-z0-9]+)(?:\s*\{|,)", command_block, re.MULTILINE)

def kebab(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "-", name).lower()

reference = root.joinpath("docs/commands/reference.mdx").read_text()
for command in map(kebab, variants):
    if f"`kache {command}`" not in reference:
        errors.append(f"command reference is missing source command: {command}")

config = root.joinpath("src/config.rs").read_text()
cache_block = config.split("struct CacheFileConfig {", 1)[1].split("/// Deliberately NOT", 1)[0]
cache_fields = re.findall(r"pub\(crate\) ([a-z0-9_]+):", cache_block)
config_doc = root.joinpath("docs/getting-started/configuration.mdx").read_text()
for field in cache_fields:
    if f"cache.{field}" not in config_doc:
        errors.append(f"configuration guide is missing source field: cache.{field}")

for key in ["cc.extra_allowlist_flags", "paths.base_dirs", "workspace.extra_inputs"]:
    if key not in config_doc:
        errors.append(f"configuration guide is missing source field: {key}")

expected_default = 'cfg!(target_os = "linux") || cfg!(target_os = "macos")'
if expected_default not in config:
    errors.append("cache_executables platform default changed; update the docs and this check")
if "Linux/macOS: `true`; Windows: `false`" not in config_doc:
    errors.append("configuration guide does not state the executable-cache platform default")

if errors:
    print("Documentation checks failed:", file=sys.stderr)
    for error in errors:
        print(f"- {error}", file=sys.stderr)
    raise SystemExit(1)

print(f"Documentation checks passed for {len(pages)} MDX pages.")
PY
