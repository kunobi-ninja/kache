//! macOS compiler admission uses the peer's kernel audit identity.

use super::TokioStream;

pub(super) fn peer_may_delegate(
    stream: &TokioStream,
    reported_pid: u32,
    trusted_pipeline: bool,
) -> bool {
    crate::transport::peer_audit_token(stream).is_ok_and(|token| {
        u32::try_from(token.pid()).ok() == Some(reported_pid)
            && crate::fallback::compiler_peer_may_delegate(token, trusted_pipeline)
            && crate::invocation::unix_broker::can_track_client(reported_pid)
    })
}
