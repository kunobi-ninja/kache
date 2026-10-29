//! Private, native argv transport for Windows engine commands that would overflow.
use anyhow::{Context, Result, ensure};
use kache_protocol::execution::NativeString;
use std::ffi::OsString;
use std::io::Read;
#[cfg(any(windows, test))]
use std::io::Write;
#[cfg(any(windows, test))]
use std::path::Path;
#[cfg(any(windows, test))]
use std::process::Command;

pub(super) const FLAG: &str = "--kache-argv-file-v1";
const MAX_BYTES: u64 = 1 << 20;

/// Keep the file open while the engine runs, including repeated argv reads.
/// Windows also deletes it if the sender is terminated without running Drop.
#[cfg(any(windows, test))]
pub(super) struct ArgvFile(tempfile::NamedTempFile);
#[cfg(not(any(windows, test)))]
pub(super) type ArgvFile = ();

#[cfg(any(windows, test))]
impl ArgvFile {
    pub(super) fn new(args: &[OsString]) -> Result<Self> {
        let mut builder = tempfile::Builder::new();
        builder.prefix("kache-argv-").suffix(".json");
        #[cfg(not(windows))]
        let file = builder.tempfile();
        #[cfg(windows)]
        let file = builder.make(|path| {
            use std::os::windows::fs::OpenOptionsExt;
            use windows_sys::Win32::Storage::FileSystem::{
                FILE_ATTRIBUTE_TEMPORARY, FILE_FLAG_DELETE_ON_CLOSE,
            };
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .custom_flags(FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE)
                .open(path)
        });
        let mut file = file.context("creating engine argv file")?;
        let native: Vec<_> = args.iter().map(|arg| NativeString::capture(arg)).collect();
        serde_json::to_writer(&mut file, &native).context("writing engine argv file")?;
        file.flush().context("flushing engine argv file")?;
        Ok(Self(file))
    }

    pub(super) fn path(&self) -> &Path {
        self.0.path()
    }
}

#[cfg(any(windows, test))]
pub(super) fn command_too_long(command: &Command, extra: &[OsString]) -> bool {
    use kache_protocol::command_line::{WINDOWS_COMMAND_LINE_LIMIT, windows_command_line_bound};
    windows_command_line_bound(
        std::iter::once(command.get_program())
            .chain(command.get_args())
            .chain(extra.iter().map(OsString::as_os_str)),
    ) > WINDOWS_COMMAND_LINE_LIMIT
}

pub(super) fn expand(args: Vec<OsString>) -> Result<Vec<OsString>> {
    if args.first().is_none_or(|arg| arg != FLAG) {
        return Ok(args);
    }
    ensure!(args.len() == 2, "invalid engine argv file envelope");
    let file = std::fs::File::open(&args[1]).context("opening engine argv file")?;
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .context("reading engine argv file")?;
    ensure!(
        bytes.len() as u64 <= MAX_BYTES,
        "engine argv file is too large"
    );
    let native: Vec<NativeString> =
        serde_json::from_slice(&bytes).context("decoding engine argv file")?;
    native
        .iter()
        .map(|arg| arg.decode().context("decoding native engine argument"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    #[test]
    fn terminating_the_sender_removes_its_argv_file_without_running_drop() {
        struct Sender(std::process::Child);
        impl Drop for Sender {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let root = tempfile::tempdir().unwrap();
        let record = root.path().join("argv-path.json");
        let mut sender = Sender(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "argv_file::tests::argv_file_sender_fixture",
                ])
                .env("KACHE_ARGV_FILE_FIXTURE", &record)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !record.exists() {
            assert!(
                sender.0.try_wait().unwrap().is_none(),
                "argv sender exited before publishing its path"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "argv sender did not publish its path"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let native: NativeString =
            serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
        let path = std::path::PathBuf::from(native.decode().unwrap());
        assert_eq!(
            expand(vec![FLAG.into(), path.clone().into_os_string()]).unwrap(),
            [OsString::from("kache")]
        );
        sender.0.kill().unwrap();
        sender.0.wait().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while path.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "kernel must delete the argv file after a hard kill"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "isolated argv-file sender for the hard-kill regression"]
    fn argv_file_sender_fixture() {
        let Some(record) = std::env::var_os("KACHE_ARGV_FILE_FIXTURE") else {
            return;
        };
        let file = ArgvFile::new(&["kache".into()]).unwrap();
        let native = NativeString::capture(file.path().as_os_str());
        let record = std::path::PathBuf::from(record);
        let pending = record.with_extension("pending");
        std::fs::write(&pending, serde_json::to_vec(&native).unwrap()).unwrap();
        std::fs::rename(pending, record).unwrap();
        let _ = std::io::stdin().read_exact(&mut [0]);
        drop(file);
    }

    #[test]
    fn windows_limit_counts_escaping_unicode_and_the_terminator() {
        for (length, expected) in [(32_758, false), (32_759, false), (32_760, true)] {
            assert_eq!(
                command_too_long(Command::new("x").arg("a".repeat(length)), &[]),
                expected
            );
        }
        let mut escaped = "a".repeat(32_756);
        escaped.push_str("\\\"");
        assert!(command_too_long(Command::new("x").arg(escaped), &[]));
        assert!(!command_too_long(
            Command::new("x").arg("🦀".repeat(16_379)),
            &[]
        ));
        assert!(command_too_long(
            Command::new("x").arg("🦀".repeat(16_380)),
            &[]
        ));
    }

    #[test]
    fn file_roundtrip_is_repeatable_native_and_lives_until_the_sender_drops() {
        #[cfg(unix)]
        let native = {
            use std::os::unix::ffi::OsStringExt;
            OsString::from_vec(vec![0xff])
        };
        #[cfg(windows)]
        let native = {
            use std::os::windows::ffi::OsStringExt;
            OsString::from_wide(&[0xd800])
        };
        let args = vec![
            "kache".into(),
            "".into(),
            "quotes\" spaces\\\n\r🦀".into(),
            native,
        ];
        let file = ArgvFile::new(&args).unwrap();
        let path = file.path().to_owned();
        let envelope = vec![FLAG.into(), path.clone().into_os_string()];
        assert_eq!(expand(envelope.clone()).unwrap(), args);
        assert_eq!(
            expand(envelope).unwrap(),
            args,
            "engine command_args may read again"
        );
        drop(file);
        assert!(!path.exists());
        assert_eq!(
            expand(vec!["direct".into()]).unwrap(),
            [OsString::from("direct")]
        );
        assert!(expand(vec![]).unwrap().is_empty());
    }

    #[test]
    fn file_envelope_rejects_missing_extra_malformed_foreign_and_nul_arguments() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let envelope = vec![FLAG.into(), file.path().into()];
        assert!(expand(vec![FLAG.into()]).is_err());
        let mut extra = envelope.clone();
        extra.push("extra".into());
        assert!(expand(extra).is_err());
        assert!(expand(vec![FLAG.into(), file.path().join("missing").into()]).is_err());
        file.write_all(b"not JSON").unwrap();
        assert!(expand(envelope.clone()).is_err());
        #[cfg(unix)]
        let foreign = NativeString::Windows(vec![65]);
        #[cfg(windows)]
        let foreign = NativeString::Unix(vec![65]);
        let nul = NativeString::capture(std::ffi::OsStr::new("\0"));
        for native in [foreign, nul] {
            std::fs::write(file.path(), serde_json::to_vec(&vec![native]).unwrap()).unwrap();
            assert!(expand(envelope.clone()).is_err());
        }
    }

    #[test]
    fn file_size_is_bounded_even_when_a_valid_json_prefix_would_parse() {
        let file = ArgvFile::new(&["kache".into()]).unwrap();
        let envelope = vec![FLAG.into(), file.path().into()];
        let mut bytes = std::fs::read(file.path()).unwrap();
        bytes.resize(1_048_576, b' ');
        std::fs::write(file.path(), &bytes).unwrap();
        assert_eq!(expand(envelope.clone()).unwrap(), [OsString::from("kache")]);
        bytes.push(b' ');
        std::fs::write(file.path(), bytes).unwrap();
        assert!(expand(envelope).is_err());
    }
}
