# kache-client

Provides Kache's thin frontend. CLI commands, daemon startup, build-script
launchers, and local fallback use the matching `kache-engine` companion. Eligible
compiler invocations can send their captured environment, working directory,
configuration, and owned streams to the local daemon.

On Linux, macOS, and Windows, the client owns native compiler launches and
brokered artifact operations while the daemon makes cache decisions. Native
adapters preserve process restrictions, descriptor/handle ownership, cancellation,
and child cleanup.

The transport prepares and authenticates a request before Commit. After the first
Commit write attempt, the caller cannot run the compiler again locally. A later
connection failure fails the invocation.

The client has no async runtime or cache-store dependency.
