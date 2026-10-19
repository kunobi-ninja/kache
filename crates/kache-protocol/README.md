# kache-protocol

Data types shared by Kache's wrapper, CLI, and local daemon. The crate preserves
the existing JSON fields and defaults. It has no filesystem, database, network,
or async runtime dependency.

It contains compile notifications, file-hash requests and results, prefetch
requests, health data, and statistics records. Configuration resolution, cache
policy, and conversion from runtime state stay with their callers.

The request and response envelopes still live in the Kache binary: publication,
prediction, and aggregate statistics payloads depend on types that have not yet
been extracted. This crate does not introduce a new protocol version.

Released with Kache under the same version number.
