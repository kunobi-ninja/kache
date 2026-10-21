//! macOS compiler admission uses the peer's kernel audit identity.

use super::TokioStream;

pub(super) fn peer_may_delegate(stream: &TokioStream, reported_pid: u32) -> bool {
    crate::transport::peer_audit_token(stream).is_ok_and(|token| {
        u32::try_from(token.pid()).ok() == Some(reported_pid)
            && crate::fallback::compiler_peer_unrestricted(token)
    })
}
