//! Discovery runs at a compiler process boundary, never on daemon process state.

use kache_protocol::execution::{
    NativeString,
    windows::{Discovery, Msvc, Tool},
};
use std::ffi::OsString;
use std::io;

struct Environment(Vec<(OsString, OsString)>);

impl find_msvc_tools::EnvGetter for Environment {
    fn get_env(&self, name: &'static str) -> Option<find_msvc_tools::Env> {
        self.0
            .iter()
            .rev()
            .find(|(key, _)| {
                key.to_str()
                    .is_some_and(|key| key.eq_ignore_ascii_case(name))
            })
            .map(|(_, value)| find_msvc_tools::Env::Owned(value.clone()))
    }
}

pub fn discover(request: &Discovery) -> io::Result<Msvc> {
    let environment = Environment(
        request
            .env
            .iter()
            .map(|(key, value)| {
                Ok((
                    key.decode().map_err(invalid)?,
                    value.decode().map_err(invalid)?,
                ))
            })
            .collect::<io::Result<_>>()?,
    );
    // The daemon's isolated banner probe resolves this before discovery. The
    // library's own architecture fallback must never execute implicit CL files.
    use find_msvc_tools::EnvGetter as _;
    if (environment.get_env("VCINSTALLDIR").is_some()
        || environment
            .get_env("VSTEL_MSBuildProjectFullPath")
            .is_some())
        && environment.get_env("VSCMD_ARG_TGT_ARCH").is_none()
    {
        return Err(invalid(
            "MSVC discovery requires an explicit developer-prompt target",
        ));
    }
    let tool = |tool: find_msvc_tools::Tool| Tool {
        path: NativeString::capture(tool.path().as_os_str()),
        env: tool
            .env()
            .into_iter()
            .map(|(key, value)| (NativeString::capture(key), NativeString::capture(value)))
            .collect(),
    };
    let linker =
        find_msvc_tools::find_tool_with_env(&request.architecture, "link.exe", &environment)
            .map(tool);
    let compiler =
        find_msvc_tools::find_tool_with_env(&request.architecture, "cl.exe", &environment)
            .map(tool);
    let sdk_version = linker
        .as_ref()
        .and_then(|_| find_msvc_tools::find_windows_sdk(&request.architecture))
        .map(|sdk| sdk.sdk_version().to_owned());
    let ucrt_version = linker
        .as_ref()
        .and_then(|_| find_msvc_tools::get_ucrt_dir())
        .map(|(_, version)| version);
    Ok(Msvc {
        linker,
        compiler,
        sdk_version,
        ucrt_version,
    })
}

fn invalid(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}
