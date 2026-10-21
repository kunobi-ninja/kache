# kache-ipc

Kache's local endpoint names, synchronous connections, and peer checks. The
`async` feature adds Tokio streams and daemon listener binding. Synchronous
clients can depend on this crate without enabling an async runtime.

Unix uses filesystem sockets. Windows derives a named-pipe name from the socket
path. Both sides use the same naming function. Listener ownership is delegated
to `kunobi-daemon`; the crate retains Kache's socket-path diagnostics and peer
admission checks.

This crate does not load Kache configuration, open its cache, or start a daemon.
Lifecycle control remains backed by `kunobi-daemon` in the application.

Unix descriptor helpers use one-use datagram tickets. On platforms without
atomic close-on-exec reception, callers hold `descriptors::spawn_guard()` through
child creation. The jobserver scope retains each request's handles and enables
inheritance in the child without changing parent descriptor flags.

Windows execution uses the same authenticated named-pipe connection for native
frontend operations. The daemon duplicates compiler process, pipe and
cancellation-job handles from the pinned client process. Client-owned child jobs
terminate compilers when the control connection disappears. A process-lifetime
job covers indirect discovery helpers until the frontend exits.

Released with Kache under the same version number.
