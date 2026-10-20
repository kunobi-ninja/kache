# kache-protocol

Data types shared by Kache's wrapper, CLI, and local daemon. The crate preserves
the existing JSON fields and defaults. It has no filesystem, database, network,
or async runtime dependency.

It contains compile notifications, file-hash requests and results, prefetch
requests, health data, and statistics records. Configuration resolution, cache
policy, and conversion from runtime state stay with their callers.

The complete request and response envelopes also live here, including publication,
prediction and aggregate statistics records. Runtime-to-record conversions and
validation stay in the application. This crate does not introduce a new protocol
version.

Released with Kache under the same version number.
