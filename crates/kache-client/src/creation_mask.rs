//! Read the caller's file creation policy without changing its process state.

/// Unknown policies cannot be delegated to another process safely.
pub fn creation_mask() -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let value = status
            .lines()
            .find_map(|line| line.strip_prefix("Umask:"))?;
        u32::from_str_radix(value.trim(), 8).ok()
    }
    #[cfg(target_os = "macos")]
    {
        native_getter()
            .map(|get| u32::from(unsafe { get() }))
            .or_else(inherited_mask)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[cfg(target_os = "macos")]
fn native_getter() -> Option<unsafe extern "C" fn() -> libc::mode_t> {
    type Getter = unsafe extern "C" fn() -> libc::mode_t;
    static GET: std::sync::OnceLock<Option<Getter>> = std::sync::OnceLock::new();
    *GET.get_or_init(|| {
        // getumask first appeared in macOS 27. Older deployment targets must
        // not acquire a load-time dependency on this symbol.
        let address = unsafe { libc::dlsym(libc::RTLD_DEFAULT, c"getumask".as_ptr()) };
        if address.is_null() {
            None
        } else {
            Some(unsafe { std::mem::transmute::<*mut libc::c_void, Getter>(address) })
        }
    })
}

#[cfg(target_os = "macos")]
fn inherited_mask() -> Option<u32> {
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;

    // A child inherits the mask. On older macOS, query it there so another
    // compiler thread never observes a temporary change to the parent mask.
    let guard = kache_ipc::descriptors::spawn_guard();
    let (mut reader, writer) = UnixStream::pair().ok()?;
    let read_fd = reader.as_raw_fd();
    let write_fd = writer.as_raw_fd();
    let pid = unsafe { libc::fork() };
    if pid == 0 {
        // Only async-signal-safe calls are allowed after a multithreaded fork.
        // Exit directly: inherited Rust owners and locks must not be dropped.
        unsafe {
            libc::close(read_fd);
            let mask = u32::from(libc::umask(0)).to_ne_bytes();
            let written = libc::write(write_fd, mask.as_ptr().cast(), mask.len());
            libc::_exit(if written == 4 { 0 } else { 1 });
        }
    }
    drop(guard);
    if pid < 0 {
        return None;
    }
    drop(writer);
    let mut bytes = [0; 4];
    let read = reader.read_exact(&mut bytes);
    let mut status = 0;
    loop {
        let result = unsafe { libc::waitpid(pid, &mut status, 0) };
        if result == pid {
            break;
        }
        if result < 0 && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        return None;
    }
    let mask = u32::from_ne_bytes(bytes);
    (read.is_ok() && status == 0 && mask <= 0o777).then_some(mask)
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn inherited_query_matches_readonly_getter_without_changing_parent() {
        let before = creation_mask().unwrap();
        std::thread::scope(|threads| {
            for _ in 0..4 {
                threads.spawn(|| assert_eq!(inherited_mask(), Some(before)));
            }
        });
        assert_eq!(creation_mask(), Some(before));
    }
}

#[cfg(all(test, target_os = "linux"))]
mod linux_tests {
    #[test]
    #[ignore = "isolated process with a known file creation mask"]
    fn inherited_mask_fixture() {
        let mask =
            u32::from_str_radix(&std::env::var("KACHE_TEST_CREATION_MASK").unwrap(), 8).unwrap();
        unsafe {
            libc::umask(mask);
        }
        assert_eq!(super::creation_mask(), Some(mask));
        assert_eq!(unsafe { libc::umask(mask) }, mask);
    }

    #[test]
    fn file_creation_policy_is_read_without_changing_the_parent_mask() {
        for mask in ["027", "137"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "creation_mask::linux_tests::inherited_mask_fixture",
                ])
                .env("KACHE_TEST_CREATION_MASK", mask)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
