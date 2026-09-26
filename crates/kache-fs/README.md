# kache-fs

[![crates.io](https://img.shields.io/crates/v/kache-fs.svg)](https://crates.io/crates/kache-fs)
[![docs.rs](https://img.shields.io/docsrs/kache-fs)](https://docs.rs/kache-fs)

Filesystem measurements, file identity, and native cloning and copying. Built for [Kache](https://github.com/kunobi-ninja/kache), a compiler cache for Rust, C/C++ and CUDA, and usable on its own. You decide where files go and when sharing an inode is acceptable; the crate reports what the filesystem does.

## What it provides

- `measure_file` separates logical length, allocated bytes and private extents. Private allocation is `None` when the platform cannot tell.
- `probe_for` picks a platform probe for directory walks. APFS uses attribute queries, including bulk directory reads. Linux uses a bounded FIEMAP walk and discards incomplete extent maps. `InodeLedger` counts hardlinks once.
- `try_reflink` creates an independent file that shares blocks where the filesystem supports it. It refuses an existing destination and returns the error so you can fall back.
- `copy_writable` copies bytes with an explicit writable mode.
- `file_identity` and `directory_identity` return the device and inode (the volume serial and file index on Windows).

## Features

| Feature | Default | Adds |
| --- | --- | --- |
| `serde` | no | Serialization for the measurement types |
| `staging` | no | `StagedFile`, a writable temporary file you publish create-only or by replacement |
| `testing` | no | Filesystem fixtures for your own tests |

`StagedFile` does not flush file contents or the parent directory when it publishes. Add that yourself where durability matters.

## Versioning

Released with Kache under the same version number.

## License

Apache-2.0
