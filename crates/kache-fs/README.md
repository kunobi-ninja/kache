# kache-fs

Filesystem measurements, file identity, native reflinks and writable copies.
Applications choose where files belong and when sharing an inode is acceptable.

`measure_file` distinguishes logical length, allocation and private extents.
Private allocation remains `None` when unavailable. `probe_for` selects a
platform probe for directory walks; an inode ledger accounts for hardlinks.
APFS uses attribute queries, including bulk directory reads. Linux uses a
bounded FIEMAP walk and discards incomplete or ambiguous extent accounting.

`try_reflink` creates an independent file that shares blocks where supported.
It refuses an existing destination and returns errors to the caller for fallback
selection. `copy_writable` copies bytes with an explicit writable mode. Hardlinks
use `std::fs::hard_link`; their content and metadata remain shared.

The `staging` feature adds `StagedFile` for writable temporary files. Callers
write through its writer, then choose create-only publication or replacement.
Unpublished stages use `tempfile`'s cleanup on drop. Creation uses ordinary
writable-file permissions, including the process umask on Unix. Callers retain
destination checks and any flush requirements; publication does not flush file
contents or the parent directory.

The default feature set is empty. `serde` enables measurement serialization;
`testing` exposes filesystem fixtures for consumers. `tempfile` is required only
with `staging`. The crate has no runtime, cache index or cleanup policy.
