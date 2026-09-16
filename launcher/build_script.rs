//! The program Cargo runs in place of a build script whose run kache caches.
//!
//! kache's own build script compiles this file for the target kache is built
//! for, and `src/build_script.rs` embeds the result and copies it over every
//! build script it caches. It is a native program because a shell does not
//! pass its environment on unchanged: dash and BusyBox ash drop every variable
//! whose name is not a shell identifier. Cargo hands a `links` dependency's
//! metadata down as `DEP_<LINKS>_<KEY>` with the key spelled as the script
//! printed it, so `cargo:core:window__PATH=...` arrives as
//! `DEP_TAURI_CORE:WINDOW__PATH`, and a shell launcher would lose it.
//!
//! It is `no_std` and calls libc directly so that each copy stays small. It
//! reads `.kache-launch` beside itself, written by kache at install time:
//! the preserved script's file name and the pinned kache's path relative to
//! the profile directory, each NUL-terminated. It execs that kache with the
//! path Cargo invoked in `KACHE_BUILD_SCRIPT_PATH`. When that kache is gone (a
//! pruned or partially restored target directory) it execs the preserved
//! script directly: uncached is always acceptable.

#![no_std]
#![no_main]

use core::ffi::{c_char, c_int, c_void};

#[cfg_attr(target_vendor = "apple", link(name = "System"))]
#[cfg_attr(not(target_vendor = "apple"), link(name = "c"))]
unsafe extern "C" {
    fn open(path: *const c_char, flags: c_int, ...) -> c_int;
    fn read(fd: c_int, buffer: *mut c_void, count: usize) -> isize;
    fn close(fd: c_int) -> c_int;
    fn access(path: *const c_char, mode: c_int) -> c_int;
    fn execve(path: *const c_char, argv: *const *const c_char, envp: *const *const c_char)
    -> c_int;
    fn malloc(size: usize) -> *mut c_void;
    fn write(fd: c_int, buffer: *const c_void, count: usize) -> isize;
    fn _exit(status: c_int) -> !;
}

const O_RDONLY: c_int = 0;
const X_OK: c_int = 1;
const PATH_CAPACITY: usize = 4096;
const SHIM_PATH_ENV: &[u8] = b"KACHE_BUILD_SCRIPT_PATH=";
const LAUNCH_RECORD: &[u8] = b".kache-launch";
/// `<profile>/build/<pkg>-<hash>/`: the profile is two levels above.
const TO_PROFILE: &[u8] = b"../../";

#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    fail(b"kache: build-script launcher panicked\n")
}

/// The precompiled `core` carries unwind tables that name this symbol. The
/// launcher aborts on panic and never unwinds, so it is never called; defining
/// it keeps the link from depending on the optimizer removing every reference.
#[unsafe(no_mangle)]
extern "C" fn rust_eh_personality() {}

/// # Safety
///
/// Called by the C runtime with the process's own `argc`, `argv` and `envp`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn main(
    argc: c_int,
    argv: *const *const c_char,
    envp: *const *const c_char,
) -> c_int {
    // SAFETY: the C runtime passes `argc` valid NUL-terminated strings in a
    // NULL-terminated `argv`, and a NULL-terminated `envp`.
    unsafe {
        let argc = usize::try_from(argc).unwrap_or(0);
        if argc == 0 {
            fail(b"kache: build-script launcher started without argv[0]\n");
        }
        let invoked = c_bytes(*argv);
        // Cargo runs the script by path; the directory prefix, slash included.
        let Some(slash) = invoked.iter().rposition(|&byte| byte == b'/') else {
            fail(b"kache: build-script launcher was not started by path\n");
        };
        let directory = &invoked[..=slash];

        let mut record = [0u8; PATH_CAPACITY];
        let record_path = join(&[directory, LAUNCH_RECORD]);
        let length = read_file(&record_path, &mut record);
        let mut fields = record[..length].split(|&byte| byte == 0);
        let (Some(real_name), Some(kache_relative)) = (fields.next(), fields.next()) else {
            fail(b"kache: build-script launcher found no .kache-launch record\n");
        };

        let kache = join(&[directory, TO_PROFILE, kache_relative]);
        if !kache_relative.is_empty() && access(kache.as_ptr().cast(), X_OK) == 0 {
            let arguments = replace_first(argv, argc, kache.as_ptr().cast());
            let environment = with_invoked_path(envp, invoked);
            execve(kache.as_ptr().cast(), arguments, environment);
        }
        let real = join(&[directory, real_name]);
        let arguments = replace_first(argv, argc, real.as_ptr().cast());
        execve(real.as_ptr().cast(), arguments, envp);
        fail(b"kache: build-script launcher could not exec kache or the preserved script\n")
    }
}

/// The bytes of a NUL-terminated string, without the NUL.
unsafe fn c_bytes<'a>(pointer: *const c_char) -> &'a [u8] {
    // SAFETY: the caller passes a valid NUL-terminated string.
    unsafe { core::ffi::CStr::from_ptr(pointer).to_bytes() }
}

/// The parts concatenated and NUL-terminated.
fn join(parts: &[&[u8]]) -> [u8; PATH_CAPACITY] {
    let mut path = [0u8; PATH_CAPACITY];
    let mut end = 0;
    for part in parts {
        // Keep the final byte for the terminator.
        if end + part.len() >= PATH_CAPACITY {
            fail(b"kache: build-script launcher path is too long\n");
        }
        path[end..end + part.len()].copy_from_slice(part);
        end += part.len();
    }
    path
}

unsafe fn read_file(path: &[u8; PATH_CAPACITY], buffer: &mut [u8]) -> usize {
    // SAFETY: `path` is NUL-terminated and `buffer` is writable for its length.
    unsafe {
        let fd = open(path.as_ptr().cast(), O_RDONLY);
        if fd < 0 {
            return 0;
        }
        let mut length = 0;
        while length < buffer.len() {
            let count = read(
                fd,
                buffer[length..].as_mut_ptr().cast(),
                buffer.len() - length,
            );
            if count <= 0 {
                break;
            }
            length += count.unsigned_abs();
        }
        close(fd);
        length
    }
}

/// `argv` with its first entry replaced, as a new NULL-terminated array.
unsafe fn replace_first(
    argv: *const *const c_char,
    argc: usize,
    first: *const c_char,
) -> *const *const c_char {
    // SAFETY: `argv` holds `argc` entries; the new array has room for them and
    // the terminator.
    unsafe {
        let copy = allocate(argc + 1);
        *copy = first;
        for index in 1..argc {
            *copy.add(index) = *argv.add(index);
        }
        *copy.add(argc) = core::ptr::null();
        copy
    }
}

/// `envp` with `KACHE_BUILD_SCRIPT_PATH` set to the invoked path. Every other
/// entry is passed on untouched, whatever its name.
unsafe fn with_invoked_path(envp: *const *const c_char, invoked: &[u8]) -> *const *const c_char {
    // SAFETY: `envp` is NULL-terminated; the new array has room for its
    // entries, the added one and the terminator.
    unsafe {
        let mut count = 0;
        while !(*envp.add(count)).is_null() {
            count += 1;
        }
        let entry = malloc(SHIM_PATH_ENV.len() + invoked.len() + 1).cast::<u8>();
        if entry.is_null() {
            fail(b"kache: build-script launcher is out of memory\n");
        }
        core::ptr::copy_nonoverlapping(SHIM_PATH_ENV.as_ptr(), entry, SHIM_PATH_ENV.len());
        core::ptr::copy_nonoverlapping(
            invoked.as_ptr(),
            entry.add(SHIM_PATH_ENV.len()),
            invoked.len(),
        );
        *entry.add(SHIM_PATH_ENV.len() + invoked.len()) = 0;

        let copy = allocate(count + 2);
        let mut kept = 0;
        for index in 0..count {
            let variable = *envp.add(index);
            if !c_bytes(variable).starts_with(SHIM_PATH_ENV) {
                *copy.add(kept) = variable;
                kept += 1;
            }
        }
        *copy.add(kept) = entry.cast();
        *copy.add(kept + 1) = core::ptr::null();
        copy
    }
}

unsafe fn allocate(entries: usize) -> *mut *const c_char {
    // SAFETY: plain allocation; a null result is handled.
    unsafe {
        let pointer =
            malloc(entries * core::mem::size_of::<*const c_char>()).cast::<*const c_char>();
        if pointer.is_null() {
            fail(b"kache: build-script launcher is out of memory\n");
        }
        pointer
    }
}

fn fail(message: &[u8]) -> ! {
    // SAFETY: writing a valid buffer to stderr, then exiting.
    unsafe {
        write(2, message.as_ptr().cast(), message.len());
        _exit(127)
    }
}
