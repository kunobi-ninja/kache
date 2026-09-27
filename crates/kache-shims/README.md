# kache-shims

[![crates.io](https://img.shields.io/crates/v/kache-shims.svg)](https://crates.io/crates/kache-shims)
[![docs.rs](https://img.shields.io/docsrs/kache-shims)](https://docs.rs/kache-shims)

Finds the path to record for a running binary so it survives an upgrade, and manages the compiler-name shim farms that point at it. Built for [Kache](https://github.com/kunobi-ninja/kache), a compiler cache for Rust, C/C++ and CUDA, and usable on its own.

## What it provides

- `detect` (or `select` with your own `Env` and `Fs`) picks the path to write into shims and service files. An installer alias comes first (Homebrew's `opt` link, a Nix profile, mise's `latest`), then a stable `PATH` entry, then the binary's own path. A candidate counts only if it reaches the same file (device and inode) as the running binary.
- `Selection` carries the installer (`Kind`), whether the path survives an upgrade (`Stability`), and a reason to show the user.
- `install` creates the symlink farm and writes the `.kache-shims` marker. In a directory it owns, it also replaces links whose target is gone, without being forced.
- `farm::status` reports a farm as active, broken, not first on `PATH`, or not installed.
- `Fs` is the read-only filesystem interface selection and status use, so each rule can be tested with fake paths.

`install` is Unix only.

## Versioning

Released with Kache under the same version number.

## License

Apache-2.0
