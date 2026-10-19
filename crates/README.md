# Crate boundaries

The application composes these libraries. A shared data type should not require
opening the cache, loading application configuration, or starting a runtime.

| Crate | Owns |
| --- | --- |
| `kache-core` | Planner data and algorithms; `planning` enables the async planner interfaces. |
| `kache-format` | Cache-entry metadata and format validation. |
| `kache-fs` | Filesystem operations, identities, and allocation measurements. |
| `kache-store` | Local artifact storage and its database. |
| `kache-shims` | Installed-binary selection and compiler-name links. |
| `kache-protocol` | Shared local-daemon request payloads and reporting records. |
| `kache-ipc` | Local endpoint naming, transport, listener ownership, and peer checks. |

`kache-protocol` uses `kache-core` with its default features disabled. It has no
storage or runtime dependency. JSON compatibility defaults belong here and must
continue to describe the messages older clients and daemons sent.

`kache-ipc` supports synchronous clients by default. Its `async` feature enables
the daemon listener and Tokio streams. It delegates socket ownership to
`kunobi-daemon`. Process startup, readiness, and replacement also use
`kunobi-daemon`, through the application's existing lifecycle adapters.

Application policy stays outside these crates: configuration resolution,
selection of cache directories, admission of planner candidates, and formatting
of CLI output. Adapters turn runtime state into protocol records.

## Remaining dependencies

The root package still builds one `kache` executable plus the embedded
build-script launcher. This extraction does not change process routing,
installation, signatures, or which process performs cache work.

The full request and response envelopes still depend on application-owned
publication events, prediction rows, and aggregate storage reports. Those
payloads need owners before the envelopes can move into `kache-protocol`.
They must retain their JSON names, omitted fields, and compatibility defaults.

Compiler invocation code also reads and modifies process-wide environment and
uses global state. Moving it into a concurrent server requires explicit
per-request inputs and ownership of cancellation, descriptors, and output.
Extracting crates alone does not make that code safe to run concurrently.

## Validation

`just check` tests the full workspace and checks the synchronous client crates
separately. The separate check prevents the daemon's enabled features from
hiding compile failures in the client-only configuration.

Protocol tests cover older messages and prevent clients from supplying internal
prefetch attribution. IPC tests cover endpoint limits, reachability, and peer
admission. Changes to execution or cache policy need their own behavioral tests
and performance measurements.
