# kache-client

Sends a captured compiler invocation and its owned streams to a running Kache
daemon. The transport prepares the request before Commit. After the first Commit
write attempt, the caller cannot run the compiler again locally.

On Windows, the client also services native process launches and console writes
over that committed connection. Cache decisions remain in the daemon.

The client has no async runtime or cache-store dependency.
