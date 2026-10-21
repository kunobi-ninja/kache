//! Admission compares the file-access context before using daemon threads.

use std::io;
use std::mem::{offset_of, size_of};
use std::os::windows::io::{AsRawHandle, BorrowedHandle, FromRawHandle, OwnedHandle};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Security::*;
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

pub(crate) fn matches_daemon(process: BorrowedHandle<'_>) -> io::Result<bool> {
    let client = token(process.as_raw_handle(), TOKEN_QUERY)?;
    let daemon = token(unsafe { GetCurrentProcess() }, TOKEN_QUERY)?;
    equivalent(client.as_raw_handle(), daemon.as_raw_handle())
}

fn token(process: HANDLE, access: u32) -> io::Result<OwnedHandle> {
    let mut handle = std::ptr::null_mut();
    if unsafe { OpenProcessToken(process, access, &mut handle) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
}

struct Information {
    storage: Vec<usize>,
    length: usize,
}

impl Information {
    fn read(token: HANDLE, class: TOKEN_INFORMATION_CLASS) -> io::Result<Self> {
        let mut length = 0;
        unsafe {
            GetTokenInformation(token, class, std::ptr::null_mut(), 0, &mut length);
        }
        // The required size is useful even when a class reports BAD_LENGTH
        // rather than INSUFFICIENT_BUFFER. The second call must still succeed.
        if length == 0 || length > (1 << 20) {
            return Err(io::Error::other("cannot size compiler security context"));
        }
        let mut storage = vec![0usize; (length as usize).div_ceil(size_of::<usize>())];
        if unsafe {
            GetTokenInformation(
                token,
                class,
                storage.as_mut_ptr().cast(),
                length,
                &mut length,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        if length as usize > storage.len() * size_of::<usize>() {
            return Err(io::Error::other(
                "compiler security context grew while reading",
            ));
        }
        Ok(Self {
            storage,
            length: length as usize,
        })
    }

    fn pointer(&self) -> *const u8 {
        self.storage.as_ptr().cast()
    }

    fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.pointer(), self.length) }
    }

    fn value<T: Copy>(&self) -> io::Result<T> {
        if self.length < size_of::<T>() {
            return Err(io::Error::other("truncated compiler security context"));
        }
        Ok(unsafe { self.pointer().cast::<T>().read_unaligned() })
    }

    fn array<T: Copy>(&self, offset: usize, count: usize) -> io::Result<Vec<T>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        if count > self.length.saturating_sub(offset) / size_of::<T>() || offset > self.length {
            return Err(io::Error::other("invalid compiler security context array"));
        }
        Ok((0..count)
            .map(|index| unsafe {
                self.pointer()
                    .add(offset + index * size_of::<T>())
                    .cast::<T>()
                    .read_unaligned()
            })
            .collect())
    }

    fn pointed_bytes(&self, pointer: *const u8, length: usize) -> io::Result<Vec<u8>> {
        let offset = (pointer as usize)
            .checked_sub(self.pointer() as usize)
            .ok_or_else(|| io::Error::other("invalid compiler security context pointer"))?;
        if offset > self.length || length > self.length - offset {
            return Err(io::Error::other(
                "compiler security context pointer is out of bounds",
            ));
        }
        Ok(self.bytes()[offset..offset + length].to_owned())
    }

    fn sid(&self, pointer: PSID) -> io::Result<Vec<u8>> {
        // Validate the SID header in the returned buffer before asking Win32
        // to read its variable-length subauthorities.
        self.pointed_bytes(pointer.cast(), 8)?;
        let length = unsafe { GetLengthSid(pointer) } as usize;
        self.pointed_bytes(pointer.cast(), length)
    }
}

fn equivalent(client: HANDLE, daemon: HANDLE) -> io::Result<bool> {
    for token in [client, daemon] {
        if Information::read(token, TokenIsAppContainer)?.value::<u32>()? != 0 {
            return Ok(false);
        }
    }
    for token in [client, daemon] {
        // Claim/capability access rules need more than user/group equality.
        // Retain local execution until those contexts can be compared fully.
        for class in [TokenUserClaimAttributes, TokenDeviceClaimAttributes] {
            if Information::read(token, class)?.array::<u32>(4, 1)?[0] != 0 {
                return Ok(false);
            }
        }
        if Information::read(token, TokenCapabilities)?.value::<u32>()? != 0 {
            return Ok(false);
        }
    }
    let client_statistics =
        Information::read(client, TokenStatistics)?.value::<TOKEN_STATISTICS>()?;
    let daemon_statistics =
        Information::read(daemon, TokenStatistics)?.value::<TOKEN_STATISTICS>()?;
    let authentication = |info: TOKEN_STATISTICS| {
        (
            info.AuthenticationId.HighPart,
            info.AuthenticationId.LowPart,
        )
    };
    if authentication(client_statistics) != authentication(daemon_statistics) {
        return Ok(false);
    }
    let same_generation = client_statistics.ModifiedId.HighPart
        == daemon_statistics.ModifiedId.HighPart
        && client_statistics.ModifiedId.LowPart == daemon_statistics.ModifiedId.LowPart;
    // Windows exposes opaque security attributes even on ordinary tokens.
    // Compare every documented access field below; permit opaque attributes
    // only on unchanged token copies with the same kernel-issued ModifiedId.
    // https://learn.microsoft.com/windows/win32/api/winnt/ns-winnt-token_statistics
    let client_access = Information::read(client, TokenAccessInformation)?;
    let daemon_access = Information::read(daemon, TokenAccessInformation)?;
    let a = client_access.value::<TOKEN_ACCESS_INFORMATION>()?;
    let b = daemon_access.value::<TOKEN_ACCESS_INFORMATION>()?;
    if ((!a.SecurityAttributes.is_null() || !b.SecurityAttributes.is_null()) && !same_generation)
        || a.TokenType != TokenPrimary
        || b.TokenType != TokenPrimary
        || a.Flags != b.Flags
        || optional_sid(&client_access, a.TrustLevelSid)?
            != optional_sid(&daemon_access, b.TrustLevelSid)?
    {
        return Ok(false);
    }
    for class in [
        TokenElevation,
        TokenElevationType,
        TokenSessionId,
        TokenUIAccess,
        TokenHasRestrictions,
        TokenVirtualizationAllowed,
        TokenVirtualizationEnabled,
        TokenMandatoryPolicy,
    ] {
        if Information::read(client, class)?.bytes() != Information::read(daemon, class)?.bytes() {
            return Ok(false);
        }
    }
    for class in [
        TokenUser,
        TokenOwner,
        TokenPrimaryGroup,
        TokenIntegrityLevel,
    ] {
        let a = Information::read(client, class)?;
        let b = Information::read(daemon, class)?;
        // All four records start with their SID pointer.
        if a.sid(a.value::<PSID>()?)? != b.sid(b.value::<PSID>()?)? {
            return Ok(false);
        }
    }
    if groups(client, TokenGroups)? != groups(daemon, TokenGroups)?
        || groups(client, TokenRestrictedSids)? != groups(daemon, TokenRestrictedSids)?
        || groups(client, TokenDeviceGroups)? != groups(daemon, TokenDeviceGroups)?
        || privileges(client)? != privileges(daemon)?
    {
        return Ok(false);
    }
    Ok(default_dacl(client)? == default_dacl(daemon)?)
}

fn optional_sid(info: &Information, pointer: PSID) -> io::Result<Option<Vec<u8>>> {
    if pointer.is_null() {
        Ok(None)
    } else {
        info.sid(pointer).map(Some)
    }
}

fn groups(token: HANDLE, class: TOKEN_INFORMATION_CLASS) -> io::Result<Vec<(Vec<u8>, u32)>> {
    let info = Information::read(token, class)?;
    let group_count = info.value::<u32>()? as usize;
    let mut groups = info
        .array::<SID_AND_ATTRIBUTES>(offset_of!(TOKEN_GROUPS, Groups), group_count)?
        .into_iter()
        .map(|group| Ok((info.sid(group.Sid)?, group.Attributes)))
        .collect::<io::Result<Vec<_>>>()?;
    groups.sort();
    Ok(groups)
}

fn privileges(token: HANDLE) -> io::Result<Vec<(i32, u32, u32)>> {
    let info = Information::read(token, TokenPrivileges)?;
    let count = info.value::<u32>()? as usize;
    let mut privileges = info
        .array::<LUID_AND_ATTRIBUTES>(offset_of!(TOKEN_PRIVILEGES, Privileges), count)?
        .into_iter()
        .map(|value| (value.Luid.HighPart, value.Luid.LowPart, value.Attributes))
        .collect::<Vec<_>>();
    privileges.sort();
    Ok(privileges)
}

fn default_dacl(token: HANDLE) -> io::Result<Option<Vec<u8>>> {
    let info = Information::read(token, TokenDefaultDacl)?;
    let pointer = info.value::<TOKEN_DEFAULT_DACL>()?.DefaultDacl;
    if pointer.is_null() {
        return Ok(None);
    }
    info.pointed_bytes(pointer.cast(), size_of::<ACL>())?;
    let length = unsafe { pointer.read_unaligned() }.AclSize as usize;
    info.pointed_bytes(pointer.cast(), length).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_primary_token_has_the_same_context() {
        let token = token(unsafe { GetCurrentProcess() }, TOKEN_QUERY).unwrap();
        if Information::read(token.as_raw_handle(), TokenIsAppContainer)
            .unwrap()
            .value::<u32>()
            .unwrap()
            == 0
        {
            assert!(equivalent(token.as_raw_handle(), token.as_raw_handle()).unwrap());
        }
    }

    #[test]
    fn an_unmodified_primary_token_copy_has_the_same_context() {
        let token = token(
            unsafe { GetCurrentProcess() },
            TOKEN_QUERY | TOKEN_DUPLICATE,
        )
        .unwrap();
        let mut duplicate = std::ptr::null_mut();
        assert_ne!(
            unsafe {
                DuplicateTokenEx(
                    token.as_raw_handle(),
                    TOKEN_QUERY,
                    std::ptr::null(),
                    SecurityImpersonation,
                    TokenPrimary,
                    &mut duplicate,
                )
            },
            0,
            "{}",
            io::Error::last_os_error()
        );
        let duplicate = unsafe { OwnedHandle::from_raw_handle(duplicate) };
        let original = Information::read(token.as_raw_handle(), TokenStatistics)
            .unwrap()
            .value::<TOKEN_STATISTICS>()
            .unwrap();
        let copied = Information::read(duplicate.as_raw_handle(), TokenStatistics)
            .unwrap()
            .value::<TOKEN_STATISTICS>()
            .unwrap();
        assert_ne!(
            (original.TokenId.HighPart, original.TokenId.LowPart),
            (copied.TokenId.HighPart, copied.TokenId.LowPart)
        );
        assert!(equivalent(token.as_raw_handle(), duplicate.as_raw_handle()).unwrap());
    }

    #[test]
    fn deleting_privileges_cannot_use_an_unrestricted_daemon() {
        let token = token(
            unsafe { GetCurrentProcess() },
            TOKEN_QUERY | TOKEN_DUPLICATE,
        )
        .unwrap();
        let mut restricted = std::ptr::null_mut();
        assert_ne!(
            unsafe {
                CreateRestrictedToken(
                    token.as_raw_handle(),
                    DISABLE_MAX_PRIVILEGE,
                    0,
                    std::ptr::null(),
                    0,
                    std::ptr::null(),
                    0,
                    std::ptr::null(),
                    &mut restricted,
                )
            },
            0
        );
        let restricted = unsafe { OwnedHandle::from_raw_handle(restricted) };
        assert!(!equivalent(restricted.as_raw_handle(), token.as_raw_handle()).unwrap());
        assert!(!equivalent(token.as_raw_handle(), restricted.as_raw_handle()).unwrap());
    }
}
