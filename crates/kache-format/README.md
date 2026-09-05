# kache-format

Entry metadata and validation shared by Kache's local store and remote readers.
The crate depends on Serde. It contains no compiler, database, or network code.

`EntryMeta` and `CachedFile` retain the existing `meta.json` representation.
The compatibility fixtures cover serialized fields and defaults for older
entries. `CACHE_KEY_VERSION` identifies the key recipe; moving its declaration
does not change that recipe.

Deserialization decodes metadata. Callers must still validate names and keys,
check artifact sizes and hashes, and enforce their import policy. Cache keys
require lowercase hexadecimal; the existing blob-hash validator also accepts
uppercase hexadecimal. Artifact-name validation follows the host's path rules.

This package follows Kache's release version. Changes must remain covered by
workspace tests, changed-line mutation testing, coverage reports, dependency
audits, and package verification.
