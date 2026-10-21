//! Native owned files for local Windows IO; delegated consoles use the frontend broker.

use super::Stdio;
use std::fs::File;
use std::io;

pub(super) fn capture() -> io::Result<Stdio> {
    use std::io::IsTerminal;
    use windows_sys::Win32::System::Console::{
        STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
    };
    let input = duplicate_standard(STD_INPUT_HANDLE)?;
    let output = duplicate_standard(STD_OUTPUT_HANDLE)?;
    let error = duplicate_standard(STD_ERROR_HANDLE)?;
    // Local owned-file IO cannot perform Rust console conversion.
    // Delegated compilation uses the frontend process broker instead.
    if input.is_terminal() || output.is_terminal() || error.is_terminal() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "console handles require local execution",
        ));
    }
    Ok(Stdio {
        input,
        output,
        error,
        terminal: None,
    })
}

fn duplicate_standard(which: u32) -> io::Result<File> {
    use std::os::windows::io::FromRawHandle;
    use windows_sys::Win32::Foundation::{
        DUPLICATE_SAME_ACCESS, DuplicateHandle, INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::System::Console::GetStdHandle;
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    let handle = unsafe { GetStdHandle(which) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::other("standard handle unavailable"));
    }
    let process = unsafe { GetCurrentProcess() };
    let mut owned = std::ptr::null_mut();
    if unsafe {
        DuplicateHandle(
            process,
            handle,
            process,
            &mut owned,
            0,
            0,
            DUPLICATE_SAME_ACCESS,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_handle(owned) })
}
