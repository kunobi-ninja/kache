#!/usr/bin/env python3
"""Discover publishable workspace crates and talk to crates.io.

Subcommands:
  list            names in dependency order (one per line)
  cargo-p-args    `-p name` flags for cargo package / publish
  status          workspace version vs crates.io vs Trusted Publishing
  bootstrap       first-publish crates that do not exist yet, then attach
                  Trusted Publishing (and optionally trustpub_only)
  publish         CI: publish each missing *version*, wait for the sparse index

Trusted Publishing cannot create a crate. `bootstrap` is the one-time claim
with a personal token (`publish-new` + `trusted-publishing`). Later versions
go through `.github/workflows/publish-crates.yaml` (OIDC).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import tomllib
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path

CRATES_IO = "https://crates.io"
INDEX = "https://index.crates.io"
USER_AGENT = "kache-crates-io (https://github.com/kunobi-ninja/kache)"
REPOSITORY_OWNER = "kunobi-ninja"
REPOSITORY_NAME = "kache"
WORKFLOW_FILENAME = "publish-crates.yaml"
# Must match the publish job: no `environment:` key.
ENVIRONMENT = None
INDEX_ATTEMPTS = 30
INDEX_SLEEP_SECS = 10


@dataclass(frozen=True)
class Crate:
    name: str
    version: str
    directory: Path
    dep_names: frozenset[str] = field(default_factory=frozenset)


def default_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_toml(path: Path) -> dict:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def is_publishable(package: dict) -> bool:
    publish = package.get("publish", True)
    if publish is False:
        return False
    if isinstance(publish, list) and len(publish) == 0:
        return False
    return True


def load_publishable(root: Path) -> list[Crate]:
    """Publishable workspace packages, including the root package, in dep order."""
    root = root.resolve()
    root_toml = load_toml(root / "Cargo.toml")
    manifests: dict[Path, dict] = {}
    package = root_toml.get("package")
    if isinstance(package, dict) and is_publishable(package):
        manifests[root] = root_toml
    for member in root_toml.get("workspace", {}).get("members", []):
        for directory in sorted(root.glob(member)):
            if not (directory / "Cargo.toml").is_file():
                continue
            manifest = load_toml(directory / "Cargo.toml")
            pkg = manifest.get("package")
            if isinstance(pkg, dict) and is_publishable(pkg):
                manifests[directory.resolve()] = manifest
    if not manifests:
        raise SystemExit("no publishable workspace crates found")

    by_dir = {}
    for directory, manifest in manifests.items():
        pkg = manifest["package"]
        by_dir[directory] = Crate(name=pkg["name"], version=pkg["version"], directory=directory)

    by_name: dict[str, Crate] = {}
    for crate in by_dir.values():
        if crate.name in by_name:
            raise SystemExit(f"duplicate publishable crate name {crate.name!r}")
        by_name[crate.name] = crate

    with_deps = []
    for crate in by_dir.values():
        manifest = manifests[crate.directory]
        dep_names = set()
        tables = [manifest.get("dependencies", {}), manifest.get("build-dependencies", {})]
        for target in manifest.get("target", {}).values():
            tables.append(target.get("dependencies", {}))
            tables.append(target.get("build-dependencies", {}))
        for dependencies in tables:
            if not isinstance(dependencies, dict):
                continue
            for name, dependency in dependencies.items():
                if not isinstance(dependency, dict) or "path" not in dependency:
                    continue
                target_dir = (crate.directory / dependency["path"]).resolve()
                target = by_dir.get(target_dir)
                if target is not None:
                    dep_names.add(target.name)
        with_deps.append(
            Crate(
                name=crate.name,
                version=crate.version,
                directory=crate.directory,
                dep_names=frozenset(dep_names),
            )
        )
    return topo_sort(with_deps)


def topo_sort(crates: list[Crate]) -> list[Crate]:
    by_name = {crate.name: crate for crate in crates}
    remaining = {crate.name: set(crate.dep_names) for crate in crates}
    ordered: list[Crate] = []
    while remaining:
        ready = sorted(name for name, deps in remaining.items() if not deps)
        if not ready:
            raise SystemExit(f"publishable crate dependency cycle: {sorted(remaining)}")
        name = ready[0]
        ordered.append(by_name[name])
        del remaining[name]
        for deps in remaining.values():
            deps.discard(name)
    return ordered


def sparse_index_path(name: str) -> str:
    """crates.io sparse-index object path. Hyphens count toward the length."""
    n = name.lower()
    if not n:
        raise ValueError("empty crate name")
    length = len(n)
    if length == 1:
        return f"1/{n}"
    if length == 2:
        return f"2/{n}"
    if length == 3:
        return f"3/{n[0]}/{n}"
    return f"{n[:2]}/{n[2:4]}/{n}"


def desired_github_config(crate_name: str) -> dict:
    return {
        "crate": crate_name,
        "repository_owner": REPOSITORY_OWNER,
        "repository_name": REPOSITORY_NAME,
        "workflow_filename": WORKFLOW_FILENAME,
        "environment": ENVIRONMENT,
    }


def env_matches(value) -> bool:
    if ENVIRONMENT is None:
        return value in (None, "")
    return value == ENVIRONMENT


def is_desired_config(config: dict) -> bool:
    return (
        config.get("repository_owner") == REPOSITORY_OWNER
        and config.get("repository_name") == REPOSITORY_NAME
        and config.get("workflow_filename") == WORKFLOW_FILENAME
        and env_matches(config.get("environment"))
    )


def format_tp(config: dict | None, *, unknown: bool = False) -> str:
    if unknown:
        return "unknown (no token)"
    if config is None:
        return "missing"
    env = config.get("environment") or "(no environment)"
    return (
        f"{config.get('repository_owner')}/{config.get('repository_name')} "
        f"{config.get('workflow_filename')} {env}"
    )


def new_crate_hint(crate_name: str) -> str:
    return (
        f"{crate_name} does not exist on crates.io yet, and Trusted Publishing "
        "cannot create crates. From a worktree at the release tag, run:\n"
        "  just crates-bootstrap\n"
        "with a crates.io token that has the `publish-new` and "
        "`trusted-publishing` scopes, then re-run Publish crates."
    )


def tp_settings_url(crate_name: str) -> str:
    return f"https://crates.io/crates/{crate_name}/settings/trusted-publishing"


def load_token() -> str | None:
    env = os.environ.get("CARGO_REGISTRY_TOKEN", "").strip()
    if env:
        return env
    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
    for name in ("credentials.toml", "credentials"):
        path = cargo_home / name
        if not path.is_file():
            continue
        try:
            data = tomllib.loads(path.read_text())
        except (OSError, tomllib.TOMLDecodeError):
            continue
        token = (
            data.get("registries", {}).get("crates-io", {}).get("token")
            or data.get("registry", {}).get("token")
        )
        if token:
            return str(token).strip()
    return None


def curl(
    method: str,
    url: str,
    *,
    token: str | None = None,
    body: dict | None = None,
    accept_404: bool = False,
    parse_json: bool = True,
) -> tuple[int, object]:
    """HTTP via curl so macOS/Python.org SSL stores are not a second failure mode."""
    cmd = [
        "curl",
        "-sS",
        "-L",
        "-A",
        USER_AGENT,
        "-X",
        method,
        "-H",
        "Accept: application/json",
        "--max-time",
        "30",
        "-w",
        "\n%{http_code}",
    ]
    if token:
        cmd.extend(["-H", f"Authorization: {token}"])
    if body is not None:
        cmd.extend(
            ["-H", "Content-Type: application/json", "--data-binary", json.dumps(body)]
        )
    cmd.append(url)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        err = result.stderr.strip() or f"exit {result.returncode}"
        raise SystemExit(f"curl failed {url}: {err}")
    stdout = result.stdout
    if "\n" in stdout:
        text, status_s = stdout.rsplit("\n", 1)
    else:
        text, status_s = "", stdout
    try:
        status = int(status_s.strip())
    except ValueError:
        raise SystemExit(f"curl returned no HTTP status for {url}") from None
    if status == 404 and accept_404:
        return 404, text
    payload: object = text
    if parse_json and text:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            payload = text
    elif parse_json and not text:
        payload = {}
    if status in (401, 403):
        detail = error_detail(payload, text)
        extra = ""
        if status == 403:
            extra = (
                "\nhint: the token may lack `trusted-publishing` / `publish-new`, "
                "or you are not an owner of this crate."
            )
        else:
            extra = "\nhint: CARGO_REGISTRY_TOKEN is missing, revoked, or stale."
        raise SystemExit(f"crates.io {status} {url}: {detail}{extra}")
    if status >= 400:
        raise SystemExit(f"crates.io {status} {url}: {text.strip() or 'request failed'}")
    return status, payload


class CratesClient:
    def __init__(self, token: str | None = None):
        self.token = token

    def request(
        self,
        method: str,
        url: str,
        *,
        body: dict | None = None,
        auth: bool = False,
        accept_404: bool = False,
        parse_json: bool = True,
    ) -> tuple[int, object]:
        token = None
        if auth:
            if not self.token:
                raise SystemExit(
                    "CARGO_REGISTRY_TOKEN is required (or `cargo login`) with the "
                    "`publish-new` and `trusted-publishing` scopes"
                )
            token = self.token
        return curl(
            method,
            url,
            token=token,
            body=body,
            accept_404=accept_404,
            parse_json=parse_json,
        )

    def crate(self, name: str) -> dict | None:
        status, payload = self.request(
            "GET",
            f"{CRATES_IO}/api/v1/crates/{urllib.parse.quote(name)}",
            accept_404=True,
        )
        if status == 404:
            return None
        if not isinstance(payload, dict):
            raise SystemExit(f"unexpected crates.io crate payload for {name}")
        return payload

    def version_exists(self, name: str, version: str) -> bool:
        status, _payload = self.request(
            "GET",
            f"{CRATES_IO}/api/v1/crates/{urllib.parse.quote(name)}/{urllib.parse.quote(version)}",
            accept_404=True,
        )
        if status == 200:
            return True
        if status == 404:
            return False
        raise SystemExit(f"unexpected crates.io status for {name} {version}: {status}")

    def github_configs(self, name: str) -> list[dict]:
        status, payload = self.request(
            "GET",
            f"{CRATES_IO}/api/v1/trusted_publishing/github_configs?crate={urllib.parse.quote(name)}",
            auth=True,
        )
        if status != 200 or not isinstance(payload, dict):
            raise SystemExit(f"failed to list Trusted Publishing configs for {name}")
        configs = payload.get("github_configs", [])
        return configs if isinstance(configs, list) else []

    def add_github_config(self, name: str) -> None:
        status, _payload = self.request(
            "POST",
            f"{CRATES_IO}/api/v1/trusted_publishing/github_configs",
            body={"github_config": desired_github_config(name)},
            auth=True,
        )
        if status not in (200, 201):
            raise SystemExit(f"failed to create Trusted Publishing config for {name}: {status}")

    def set_trustpub_only(self, name: str, enabled: bool) -> None:
        status, _payload = self.request(
            "PATCH",
            f"{CRATES_IO}/api/v1/crates/{urllib.parse.quote(name)}",
            body={"crate": {"trustpub_only": enabled}},
            auth=True,
        )
        if status != 200:
            raise SystemExit(
                f"failed to set trustpub_only={enabled} on {name}: {status}\n"
                f"set it in the UI: {tp_settings_url(name)}"
            )

    def index_has_version(self, name: str, version: str) -> bool:
        url = f"{INDEX}/{sparse_index_path(name)}"
        status, payload = self.request(
            "GET", url, accept_404=True, parse_json=False
        )
        if status == 404:
            return False
        if status != 200:
            raise SystemExit(f"unexpected sparse-index status for {name}: {status}")
        text = payload if isinstance(payload, str) else str(payload)
        return f'"vers":"{version}"' in text


def error_detail(payload: object, fallback: str) -> str:
    if isinstance(payload, dict):
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            parts = []
            for item in errors:
                if isinstance(item, dict) and item.get("detail"):
                    parts.append(str(item["detail"]))
            if parts:
                return " ".join(parts)
    return fallback.strip() or "request failed"


def cargo_p_args(crates: list[Crate]) -> list[str]:
    args: list[str] = []
    for crate in crates:
        args.extend(["-p", crate.name])
    return args


def git(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        text=True,
        capture_output=True,
        check=False,
    )


def require_clean_tree(root: Path) -> None:
    result = git(root, "status", "--porcelain")
    if result.returncode != 0:
        raise SystemExit(result.stderr.strip() or "git status failed")
    if result.stdout.strip():
        raise SystemExit("working tree is dirty — commit or stash first")


def tagged_as(root: Path, version: str) -> bool:
    result = git(root, "describe", "--tags", "--exact-match", "HEAD")
    if result.returncode != 0:
        return False
    return result.stdout.strip() == f"v{version}"


def run_cargo(root: Path, args: list[str]) -> None:
    result = subprocess.run(["cargo", *args], cwd=root)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def wait_for_index(
    client: CratesClient,
    name: str,
    version: str,
    *,
    attempts: int = INDEX_ATTEMPTS,
    sleep_secs: float = INDEX_SLEEP_SECS,
    sleep=time.sleep,
) -> None:
    for attempt in range(1, attempts + 1):
        if client.index_has_version(name, version):
            print(f"{name} {version} visible in crates.io sparse index")
            return
        print(f"{name} {version} not visible yet; retry {attempt}/{attempts}")
        sleep(sleep_secs)
    raise SystemExit(f"{name} {version} did not become visible in crates.io index")


def cmd_list(crates: list[Crate]) -> int:
    for crate in crates:
        print(crate.name)
    return 0


def cmd_cargo_p_args(crates: list[Crate]) -> int:
    print(" ".join(cargo_p_args(crates)))
    return 0


def cmd_status(crates: list[Crate], client: CratesClient, *, check: bool) -> int:
    token = client.token
    problems = 0
    width = max(len(crate.name) for crate in crates)
    for crate in crates:
        payload = client.crate(crate.name)
        if payload is None:
            registry = "absent"
            trustpub = "—"
            tp = "—"
            problems += 1
        else:
            info = payload.get("crate", {})
            max_version = info.get("max_version") if isinstance(info, dict) else None
            trustpub_only = info.get("trustpub_only") if isinstance(info, dict) else None
            registry = str(max_version or "?")
            trustpub = "yes" if trustpub_only else "no"
            if token:
                configs = client.github_configs(crate.name)
                desired = next((c for c in configs if is_desired_config(c)), None)
                tp = format_tp(desired)
                if desired is None:
                    problems += 1
            else:
                tp = format_tp(None, unknown=True)
            if max_version != crate.version:
                problems += 1
        print(
            f"{crate.name:<{width}}  workspace={crate.version}  "
            f"crates.io={registry}  tp={tp}  trustpub_only={trustpub}"
        )
    if check and problems:
        return 1
    return 0


def cmd_bootstrap(
    crates: list[Crate],
    client: CratesClient,
    root: Path,
    *,
    dry_run: bool,
    allow_untagged: bool,
    no_trustpub_only: bool,
    sleep=time.sleep,
) -> int:
    version = crates[0].version
    if any(crate.version != version for crate in crates):
        raise SystemExit("publishable crate versions are not uniform; run `just bump`")
    if not client.token:
        raise SystemExit(
            "CARGO_REGISTRY_TOKEN is required (or `cargo login`) with the "
            "`publish-new` and `trusted-publishing` scopes"
        )
    if not dry_run:
        require_clean_tree(root)
        if not allow_untagged and not tagged_as(root, version):
            raise SystemExit(
                f"HEAD is not tag v{version} — first-publish from the release tag "
                "(or pass --allow-untagged)"
            )

    published_any = False
    for crate in crates:
        payload = client.crate(crate.name)
        exists = payload is not None
        trustpub_only = False
        if exists:
            info = payload.get("crate", {})
            if isinstance(info, dict):
                trustpub_only = bool(info.get("trustpub_only"))
        configs: list[dict] = []
        if exists:
            configs = client.github_configs(crate.name)
        desired = next((c for c in configs if is_desired_config(c)), None)

        actions: list[str] = []
        need_publish = not exists
        need_tp = desired is None
        need_only = exists and not trustpub_only and not no_trustpub_only
        # Enable trustpub_only after the crate exists (publish this loop or already).
        if need_publish and not no_trustpub_only:
            need_only = True
        if need_publish:
            actions.append(f"cargo publish -p {crate.name} --locked")
        if need_tp:
            actions.append(
                f"add Trusted Publishing {REPOSITORY_OWNER}/{REPOSITORY_NAME} "
                f"{WORKFLOW_FILENAME} (no environment)"
            )
        if need_only:
            actions.append("enable trustpub_only")
        if not actions:
            print(f"{crate.name}: ok")
            continue
        if dry_run:
            print(f"{crate.name}: would " + "; ".join(actions))
            continue

        if need_publish:
            if published_any:
                sleep(INDEX_SLEEP_SECS)
            print(f"{crate.name}: publishing first version {crate.version}")
            run_cargo(root, ["publish", "-p", crate.name, "--locked"])
            wait_for_index(client, crate.name, crate.version, sleep=sleep)
            published_any = True
        if need_tp:
            print(f"{crate.name}: registering Trusted Publishing")
            try:
                client.add_github_config(crate.name)
            except SystemExit as error:
                print(error, file=sys.stderr)
                print(f"configure it in the UI: {tp_settings_url(crate.name)}", file=sys.stderr)
                return 1
        if need_only:
            print(f"{crate.name}: enabling trustpub_only")
            client.set_trustpub_only(crate.name, True)
        print(f"{crate.name}: ok")
    return 0


def cmd_publish(
    crates: list[Crate],
    client: CratesClient,
    root: Path,
    *,
    version: str,
    dry_run: bool,
    sleep=time.sleep,
) -> int:
    if any(crate.version != version for crate in crates):
        raise SystemExit(
            f"workspace crates are not all {version}; run check-version-consistency.sh first"
        )
    for crate in crates:
        if client.version_exists(crate.name, version):
            print(f"{crate.name} {version} is already published")
        else:
            payload = client.crate(crate.name)
            if payload is None:
                print(new_crate_hint(crate.name), file=sys.stderr)
                return 1
            if dry_run:
                print(f"{crate.name}: would cargo publish -p {crate.name} --locked")
            else:
                print(f"{crate.name}: publishing {version}")
                run_cargo(root, ["publish", "-p", crate.name, "--locked"])
        if not dry_run:
            wait_for_index(client, crate.name, version, sleep=sleep)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=default_root(),
        help="workspace root (default: repository root)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", help="print publishable crate names in dependency order")
    sub.add_parser("cargo-p-args", help="print -p flags for cargo package/publish")
    status = sub.add_parser("status", help="compare workspace crates to crates.io")
    status.add_argument(
        "--check",
        action="store_true",
        help="exit 1 when a crate is missing, behind, or missing Trusted Publishing",
    )
    bootstrap = sub.add_parser(
        "bootstrap",
        help="first-publish new crates and attach Trusted Publishing",
    )
    bootstrap.add_argument("--dry-run", action="store_true")
    bootstrap.add_argument(
        "--allow-untagged",
        action="store_true",
        help="allow first-publish from a commit that is not tag v<version>",
    )
    bootstrap.add_argument(
        "--no-trustpub-only",
        action="store_true",
        help="do not enable 'require Trusted Publishing for new versions'",
    )
    publish = sub.add_parser("publish", help="publish missing versions (CI)")
    publish.add_argument("--version", required=True, help="version to publish, e.g. 0.17.0")
    publish.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    root = args.root.resolve()
    crates = load_publishable(root)
    if args.cmd == "list":
        return cmd_list(crates)
    if args.cmd == "cargo-p-args":
        return cmd_cargo_p_args(crates)

    token = load_token()
    client = CratesClient(token=token)
    if args.cmd == "status":
        return cmd_status(crates, client, check=args.check)
    if args.cmd == "bootstrap":
        return cmd_bootstrap(
            crates,
            client,
            root,
            dry_run=args.dry_run,
            allow_untagged=args.allow_untagged,
            no_trustpub_only=args.no_trustpub_only,
        )
    if args.cmd == "publish":
        return cmd_publish(
            crates,
            client,
            root,
            version=args.version,
            dry_run=args.dry_run,
        )
    parser.error(f"unknown command {args.cmd}")
    return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
