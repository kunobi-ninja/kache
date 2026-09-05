# kache-store

Local artifact storage for Kache: content-addressed blobs, entry metadata,
SQLite indexing, file fingerprints, locks, reclamation, and filesystem linking.

`ArtifactStore<P>` uses storage options and an `ArtifactPolicy` supplied by its
caller. The policy decides which outputs may share an inode, which may be empty,
which emit kinds they satisfy, and whether their hashes remain valid after
publication. Kache implements those rules using its compiler artifact model.

Compiler parsing, daemon communication, and remote transport stay outside this
crate. The durable upload spool stores publication intents; the caller performs
the transfer. Disk formats and cache-key schemas come from `kache-format`.

The `test-support` feature exposes fixture helpers for Kache's integration with
the store. Ordinary builds do not enable it.
