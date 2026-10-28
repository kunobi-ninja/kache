# Crate boundaries

The application composes these libraries. A shared data type should not require
opening the cache, loading application configuration, or starting a runtime.

| Crate | Owns |
| --- | --- |
| `kache-client` | Thin frontend routing, captured invocations, synchronous handoff, and client-owned compiler/restore execution. |
| `kache-config` | Configuration resolution from files and a captured invocation environment. |
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

Configuration and cache/runtime path resolution live in `kache-config`.
Application policy such as planner admission, pipeline orchestration, service
installation, and CLI presentation stays in the engine. Adapters turn runtime
state into protocol records.

## Application processes

The root package installs two matching executables: `kache` from
`src/frontend.rs`, and `kache-engine` from `src/engine.rs`. The application library
is rooted at `src/main.rs`. The frontend routes CLI commands to the engine and
hands eligible compiler requests to a running daemon; the engine handles startup
and local fallback. Build-script launchers remain embedded in the application.

For committed daemon requests, native child processes and brokered artifact
operations stay in the client on Linux, macOS, and Windows. The daemon runs
concurrent cache pipelines with explicit per-invocation environment, working
directory, configuration, streams, jobserver, cancellation, and accounting.

The complete request and response envelopes live in `kache-protocol`, including
publication events, prediction rows, and aggregate storage reports. Store memo
inputs convert at the application boundary; storage access and validation remain
with the application. JSON names, omitted fields, and compatibility defaults
remain unchanged.

Compiler work must use those request inputs rather than changing the daemon's
process-wide environment or current directory. Preparation can refuse delegation
and retain local fallback. The first Commit write attempt transfers execution
ownership; a later disconnect must not start a second compiler.

See the [architecture guide](../docs/how-it-works/architecture.mdx) for routing,
process restrictions, and the local/remote cache path.

## Validation

`just check` tests the full workspace and checks the synchronous client crates
separately. The separate check prevents the daemon's enabled features from
hiding compile failures in the client-only configuration.

Protocol tests cover older messages and prevent clients from supplying internal
prefetch attribution. IPC tests cover endpoint limits, reachability, and peer
admission. Changes to execution or cache policy need their own behavioral tests
and performance measurements.
