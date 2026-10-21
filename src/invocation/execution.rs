//! Adapt engine-owned streams to the shared synchronous client.

use super::InvocationContext;
use anyhow::Result;
use std::path::Path;

pub(crate) fn try_execute(
    socket: &Path,
    args: &[String],
    context: &InvocationContext,
) -> Result<Option<i32>> {
    #[cfg(unix)]
    let (files, terminal) = match context.io.transfer_files() {
        Ok(files) => files,
        Err(error) => {
            tracing::debug!(%error, "compiler streams cannot be transferred");
            return Ok(None);
        }
    };
    #[cfg(windows)]
    let (files, terminal) = (Vec::new(), false);
    let executable = match crate::platform::frontend_executable() {
        Ok(path) => path,
        Err(error) => {
            tracing::debug!(%error, "compiler client executable cannot be resolved");
            return Ok(None);
        }
    };
    let args = args
        .iter()
        .map(std::ffi::OsString::from)
        .collect::<Vec<_>>();
    kache_client::Execution {
        entry: kache_protocol::execution::ExecutionEntry::Compiler,
        args: &args,
        env: &context.env,
        started_at: context.started_at,
        executable: &executable,
        files: &files,
        terminal,
        client_version: crate::VERSION,
    }
    .try_execute(socket)
}
