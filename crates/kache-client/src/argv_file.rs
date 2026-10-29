//! Private, native argv transport for Windows engine commands that would overflow.
use anyhow::{Context, Result, ensure};
use kache_protocol::execution::NativeString;
use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::Path;
use std::process::Command;

pub(super) const FLAG: &str = "--kache-argv-file-v1";
const MAX_BYTES: u64 = 1 << 20;

/// The frontend retains the file until the engine exits, including on spawn errors.
/// Keep it available for later calls to `engine_entry` in the same engine process.
pub(super) struct ArgvFile(tempfile::NamedTempFile);

impl ArgvFile {
    pub(super) fn new(args: &[OsString]) -> Result<Self> {
        let mut file = tempfile::Builder::new()
            .prefix("kache-argv-")
            .suffix(".json")
            .tempfile()
            .context("creating engine argv file")?;
        let native: Vec<_> = args.iter().map(|arg| NativeString::capture(arg)).collect();
        serde_json::to_writer(&mut file, &native).context("writing engine argv file")?;
        file.flush().context("flushing engine argv file")?;
        Ok(Self(file))
    }

    pub(super) fn path(&self) -> &Path {
        self.0.path()
    }
}

/// Conservative UTF-16 bound: quote every word and escape every backslash/quote.
/// Include separators and the terminating NUL; preserve native Windows units.
pub(super) fn command_too_long(command: &Command) -> bool {
    let bound = std::iter::once(command.get_program())
        .chain(command.get_args())
        .fold(1_usize, |total, word| {
            let native = NativeString::capture(word);
            let units: Vec<u16> = match native {
                NativeString::Windows(units) => units,
                NativeString::Unix(_) => word.to_string_lossy().encode_utf16().collect(),
            };
            total
                + units.len()
                + units
                    .iter()
                    .filter(|&&u| u == u16::from(b'"') || u == u16::from(b'\\'))
                    .count()
                + 3
        });
    bound > 32_767
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

    #[test]
    fn windows_limit_counts_escaping_unicode_and_the_terminator() {
        for (length, expected) in [(32_758, false), (32_759, false), (32_760, true)] {
            assert_eq!(
                command_too_long(Command::new("x").arg("a".repeat(length))),
                expected
            );
        }
        let mut escaped = "a".repeat(32_756);
        escaped.push_str("\\\"");
        assert!(command_too_long(Command::new("x").arg(escaped)));
        assert!(!command_too_long(
            Command::new("x").arg("🦀".repeat(16_379))
        ));
        assert!(command_too_long(Command::new("x").arg("🦀".repeat(16_380))));
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
