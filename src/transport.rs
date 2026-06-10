//! Cross-platform local IPC transport for the daemon.
//!
//! The daemon's local socket and the wrapper / CLI clients use the
//! `interprocess` crate so the same API works on Unix domain sockets and
//! Windows named pipes. This module hides the `Name` resolution machinery
//! and provides a `prelude` with the trait imports callers need.
//!
//! # Naming
//!
//! kache addresses the daemon by a filesystem path (e.g.
//! `~/.cache/kache/kache-daemon.sock`). On Unix that becomes the literal
//! UDS path. On Windows the path is translated to a named pipe under
//! `\\.\pipe\` by `interprocess`.

#![allow(dead_code)] // wired into daemon.rs incrementally; rm after migration

use anyhow::{Context, Result};
use std::path::Path;

pub use interprocess::local_socket::tokio::Stream as TokioStream;
pub use interprocess::local_socket::{ListenerOptions, Name, Stream as SyncStream};
// TokioListener is the type produced by `ListenerOptions::new().create_tokio()`.
// Re-exported so the daemon's `accept_loop` and its tests can name it explicitly.
pub use interprocess::local_socket::tokio::Listener as TokioListener;

/// Trait imports needed to call methods on the listener / stream types.
/// Most users want `use crate::transport::prelude::*;`.
pub mod prelude {
    pub use interprocess::local_socket::traits::tokio::{Listener as _, Stream as _};
    pub use interprocess::local_socket::traits::{Listener as _, Stream as _};
}

/// Build the platform-appropriate IPC name for a path.
///
/// On Unix, the path is used directly as a filesystem UDS path via
/// `GenericFilePath`. On Windows, the path cannot be used as a named pipe
/// path directly (named pipes must live under `\\.\pipe\`), so we derive a
/// namespaced name from the path via `GenericNamespaced`.
pub fn socket_name(path: &Path) -> Result<Name<'static>> {
    #[cfg(unix)]
    {
        use interprocess::local_socket::{GenericFilePath, ToFsName};
        path.to_fs_name::<GenericFilePath>()
            .with_context(|| format!("converting {} to local-socket name", path.display()))
            .map(|n| n.into_owned())
    }
    #[cfg(windows)]
    {
        use interprocess::local_socket::{GenericNamespaced, ToNsName};
        // Derive a stable pipe name from the socket path so different
        // cache dirs get different pipes.
        let hash = blake3::hash(path.as_os_str().as_encoded_bytes());
        let short = &hash.to_hex()[..16];
        let pipe_name = format!("kache-daemon-{short}");
        pipe_name
            .to_ns_name::<GenericNamespaced>()
            .with_context(|| format!("converting {} to named pipe name", path.display()))
            .map(|n| n.into_owned())
    }
}

/// True if a daemon socket / named pipe at `path` accepts a connection
/// right now. Used by liveness probes — does not check whether the daemon
/// is responsive, only whether *something* is listening.
pub fn is_reachable(path: &Path) -> bool {
    use interprocess::local_socket::traits::Stream as _;
    let Ok(name) = socket_name(path) else {
        return false;
    };
    SyncStream::connect(name).is_ok()
}

/// True for I/O errors that mean the peer disconnected. Cross-platform —
/// covers Unix `BrokenPipe` / `ConnectionReset` / `EPIPE` and the
/// Windows-pipe variants of the same.
pub fn is_peer_disconnect(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;
    matches!(
        e.kind(),
        BrokenPipe | ConnectionReset | ConnectionAborted | UnexpectedEof
    ) || e.raw_os_error() == Some(32) // EPIPE; macOS sometimes reports as ErrorKind::Other
}
