# kache-store

[![crates.io](https://img.shields.io/crates/v/kache-store.svg)](https://crates.io/crates/kache-store)
[![docs.rs](https://img.shields.io/docsrs/kache-store)](https://docs.rs/kache-store)

Local artifact storage for [Kache](https://github.com/kunobi-ninja/kache), a compiler cache for Rust, C/C++ and CUDA: content-addressed blobs, entry metadata, a SQLite index, file fingerprints, locks, reclamation and linking into build directories.

## What it provides

- `ArtifactStore<P>`: the store itself. An `ArtifactPolicy` you supply decides which outputs may share an inode, which may be empty, which emit kinds they satisfy, and whether their hashes stay valid after publication.
- `link`: restoring blobs into a build directory by reflink, hardlink or copy.
- `eviction`: choosing which entries garbage collection removes.
- `file_hash`: fingerprints of input files, kept across builds.
- A durable upload spool that records what should be published. The caller does the transfer.

Compiler parsing, daemon communication and remote transport live outside this crate. Disk formats and the cache-key version come from [`kache-format`](https://crates.io/crates/kache-format).

## Features

| Feature | Default | Adds |
| --- | --- | --- |
| `test-support` | no | Fixture helpers for tests that drive the store |

## Versioning

Released with Kache under the same version number.

## License

Apache-2.0
