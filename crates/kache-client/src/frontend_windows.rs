//! Start a cold daemon while retaining compiler creation in the frontend.

use crate::Execution;
use anyhow::Result;
use std::path::Path;
use std::process::Stdio;

pub(super) fn try_bootstrap(
    execution: &Execution<'_>,
    socket: &Path,
    disabled: bool,
) -> Result<Option<i32>> {
    let bootstrap = ["kache".into(), "daemon".into(), "start".into()];
    if !disabled
        && let Ok(mut command) = super::engine_command(
            execution.executable,
            execution.client_version,
            &bootstrap,
            execution.started_at,
        )
        && command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    {
        execution.try_execute(socket)
    } else {
        Ok(None)
    }
}
