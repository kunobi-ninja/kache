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

Released with Kache under the same version number.
