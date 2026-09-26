# kache-format

[![crates.io](https://img.shields.io/crates/v/kache-format.svg)](https://crates.io/crates/kache-format)
[![docs.rs](https://img.shields.io/docsrs/kache-format)](https://docs.rs/kache-format)

Cache-entry metadata and validation for [Kache](https://github.com/kunobi-ninja/kache), a compiler cache for Rust, C/C++ and CUDA. The local store and the remote readers share these types, so both read and write the same `meta.json`.

## What it provides

- `EntryMeta` and `CachedFile`: the metadata stored next to each cached artifact. Compatibility fixtures pin the serialized fields and the defaults older entries rely on.
- `CACHE_KEY_VERSION`: the version of the cache-key recipe written into every entry.
- Validators for cache keys, crate names, blob hashes and artifact names. Artifact-name checks follow the host's path rules.

The crate depends only on Serde. It has no compiler, database or network code.

Deserializing an entry only decodes it. Callers still validate names and keys, check artifact sizes and hashes, and apply their own import policy.

## Versioning

Released with Kache under the same version number.

## License

Apache-2.0
