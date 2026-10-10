//! Install names copied by ld64 into linked outputs.

use anyhow::{Context, Result, bail, ensure};
use std::io::Read;
use std::path::Path;

const MAX_HEADER: usize = 1 << 20;

pub(super) fn read(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("reading Apple install name from {}", path.display()))?;
    let name = if path.extension().is_some_and(|ext| ext == "tbd") {
        let mut bytes = Vec::new();
        file.take(MAX_HEADER as u64 + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() <= MAX_HEADER,
            "text stub exceeds install-name parser limit"
        );
        stub(std::str::from_utf8(&bytes)?)
    } else {
        let mut bytes = vec![0; 32];
        file.read_exact(&mut bytes)?;
        let (header, word) = layout(&bytes)?;
        let commands = word(&bytes[20..24]) as usize;
        ensure!(
            commands <= MAX_HEADER - header,
            "Mach-O load commands exceed parser limit"
        );
        let length = header + commands;
        ensure!(
            length >= bytes.len(),
            "Mach-O has no complete load commands"
        );
        bytes.resize(length, 0);
        file.read_exact(&mut bytes[32..])?;
        macho(&bytes)
    };
    name.with_context(|| format!("unmodeled Apple install name in {}", path.display()))
}

fn stub(text: &str) -> Result<String> {
    ensure!(
        text.trim_start().starts_with("--- !tapi"),
        "unsupported text stub format"
    );
    for line in text
        .lines()
        .filter(|line| !line.trim_start().starts_with('#'))
    {
        if let Some((key, _)) = line.split_once(':') {
            ensure!(
                !key.contains(['\'', '"', '?', '&', '*', '{', '}']) && key.trim() != "<<",
                "complex stub key is unsupported"
            );
        }
        ensure!(
            !line.contains("install-name") || line.trim().starts_with("install-name:"),
            "unsupported install-name field spelling"
        );
    }
    let mut names = text
        .lines()
        .filter_map(|line| line.trim().strip_prefix("install-name:").map(str::trim));
    let value = names.next().context("text stub has no install-name")?;
    ensure!(
        names.next().is_none(),
        "text stub has multiple install names"
    );
    let value = if let Some(value) = value.strip_prefix('\'') {
        let value = value
            .strip_suffix('\'')
            .context("unterminated stub install name")?;
        ensure!(
            !value.contains('\''),
            "escaped stub install name is unsupported"
        );
        value
    } else if let Some(value) = value.strip_prefix('"') {
        let value = value
            .strip_suffix('"')
            .context("unterminated stub install name")?;
        ensure!(
            !value.contains(['"', '\\']),
            "escaped stub install name is unsupported"
        );
        value
    } else {
        ensure!(
            !value.contains(char::is_whitespace),
            "complex stub install name is unsupported"
        );
        value
    };
    ensure!(
        !value.is_empty()
            && !value.contains(['#', '\0'])
            && !value.starts_with(['*', '&', '|', '>']),
        "invalid stub install name"
    );
    Ok(value.to_owned())
}

type Word = fn(&[u8]) -> u32;

fn layout(bytes: &[u8]) -> Result<(usize, Word)> {
    let little = |bytes: &[u8]| u32::from_le_bytes(bytes.try_into().unwrap());
    let big = |bytes: &[u8]| u32::from_be_bytes(bytes.try_into().unwrap());
    match bytes.get(..4) {
        Some([0xce, 0xfa, 0xed, 0xfe]) => Ok((28, little)),
        Some([0xcf, 0xfa, 0xed, 0xfe]) => Ok((32, little)),
        Some([0xfe, 0xed, 0xfa, 0xce]) => Ok((28, big)),
        Some([0xfe, 0xed, 0xfa, 0xcf]) => Ok((32, big)),
        _ => bail!("unsupported Mach-O header (including universal binaries)"),
    }
}

fn macho(bytes: &[u8]) -> Result<String> {
    let (header, word) = layout(bytes)?;
    ensure!(bytes.len() >= header, "truncated Mach-O header");
    ensure!(word(&bytes[12..16]) == 6, "Mach-O input is not a dylib");
    let count = word(&bytes[16..20]) as usize;
    let size = word(&bytes[20..24]) as usize;
    ensure!(
        size <= MAX_HEADER - header && count <= size / 8,
        "invalid Mach-O command bounds"
    );
    let commands = bytes
        .get(header..header + size)
        .context("truncated Mach-O load commands")?;
    let mut offset = 0;
    let mut name = None;
    for _ in 0..count {
        let command = commands
            .get(offset..offset + 8)
            .context("truncated Mach-O load command")?;
        let kind = word(&command[..4]);
        let length = word(&command[4..8]) as usize;
        ensure!(
            length >= 8 && length <= commands.len() - offset,
            "invalid Mach-O command size"
        );
        let command = &commands[offset..offset + length];
        if kind == 0xd {
            ensure!(
                name.is_none() && length >= 24,
                "invalid LC_ID_DYLIB command"
            );
            let start = word(&command[8..12]) as usize;
            ensure!(
                start >= 24 && start < length,
                "invalid LC_ID_DYLIB name offset"
            );
            let bytes = &command[start..];
            let end = bytes
                .iter()
                .position(|byte| *byte == 0)
                .context("unterminated dylib install name")?;
            ensure!(end > 0, "empty dylib install name");
            name = Some(std::str::from_utf8(&bytes[..end])?.to_owned());
        }
        offset += length;
    }
    ensure!(offset == commands.len(), "unparsed Mach-O load commands");
    name.context("Mach-O has no LC_ID_DYLIB")
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    pub(crate) fn dylib(name: &str, big: bool, wide: bool) -> Vec<u8> {
        let header = if wide { 32 } else { 28 };
        let length = (24 + name.len() + 1).next_multiple_of(8);
        let word = |value: u32| {
            if big {
                value.to_be_bytes()
            } else {
                value.to_le_bytes()
            }
        };
        let mut bytes = vec![0; header + length];
        bytes[..4].copy_from_slice(&word(if wide { 0xfeedfacf } else { 0xfeedface }));
        bytes[12..16].copy_from_slice(&word(6));
        bytes[16..20].copy_from_slice(&word(1));
        bytes[20..24].copy_from_slice(&word(length as u32));
        bytes[header..header + 4].copy_from_slice(&word(0xd));
        bytes[header + 4..header + 8].copy_from_slice(&word(length as u32));
        bytes[header + 8..header + 12].copy_from_slice(&word(24));
        bytes[header + 24..header + 24 + name.len()].copy_from_slice(name.as_bytes());
        bytes
    }

    #[test]
    fn stub_reads_plain_and_quoted_names() {
        for value in [
            "/a/libfoo.dylib",
            "'/a path/libfoo.dylib'",
            "\"@rpath/libfoo.dylib\"",
        ] {
            assert_eq!(
                stub(&format!("--- !tapi-tbd\ninstall-name: {value}\n...\n")).unwrap(),
                value.trim_matches(['\'', '"'])
            );
        }
    }

    #[test]
    fn stub_refuses_ambiguous_and_complex_names() {
        for value in [
            "",
            "*alias",
            "'unterminated",
            "\"escaped\\name\"",
            "'/a''b'",
            "/a # comment",
            "/a\ninstall-name: /b",
        ] {
            assert!(
                stub(&format!("--- !tapi-tbd\ninstall-name: {value}\n")).is_err(),
                "{value:?}"
            );
        }
        assert!(stub("{\"install-name\":\"/a\"}").is_err());
        assert!(stub("--- !tapi-tbd\nexports: []\n").is_err());
        assert!(stub("--- !tapi-tbd\ninstall-name: /a\n\"install-name\": /b\n").is_err());
        assert!(stub("--- !tapi-tbd\ninstall-name: /a\n\"install\\u002dname\": /b\n").is_err());
    }

    #[test]
    fn macho_reads_both_widths_and_byte_orders() {
        for big in [false, true] {
            for wide in [false, true] {
                let bytes = dylib("/target/libfoo.dylib", big, wide);
                assert_eq!(macho(&bytes).unwrap(), "/target/libfoo.dylib");
                for length in 0..bytes.len() {
                    assert!(macho(&bytes[..length]).is_err(), "{length}");
                }
            }
        }
    }

    #[test]
    fn macho_refuses_invalid_load_commands() {
        let original = dylib("/a", false, true);
        for (offset, value) in [
            (12, 1),
            (16, u32::MAX),
            (20, u32::MAX),
            (36, 7),
            (40, 0),
            (40, 4096),
            (32, 1),
        ] {
            let mut bytes = original.clone();
            bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            assert!(macho(&bytes).is_err(), "{offset}={value}");
        }
        let mut bytes = original;
        bytes[56..].fill(b'x');
        assert!(macho(&bytes).is_err());
        assert!(macho(&[0xca, 0xfe, 0xba, 0xbe]).is_err());
    }

    #[test]
    fn file_reader_bounds_stubs_and_reads_only_dylib_headers() {
        let dir = tempfile::tempdir().unwrap();
        let library = dir.path().join("libfoo.dylib");
        std::fs::write(&library, dylib("@rpath/libfoo.dylib", false, true)).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&library)
            .unwrap()
            .set_len(1 << 28)
            .unwrap();
        assert_eq!(read(&library).unwrap(), "@rpath/libfoo.dylib");
        let stub = dir.path().join("libfoo.tbd");
        let mut text = b"--- !tapi-tbd\ninstall-name: @rpath/libfoo.dylib\n".to_vec();
        text.resize(MAX_HEADER, b' ');
        std::fs::write(&stub, &text).unwrap();
        assert_eq!(read(&stub).unwrap(), "@rpath/libfoo.dylib");
        text.push(b' ');
        std::fs::write(&stub, text).unwrap();
        assert!(read(&stub).is_err());
        assert!(read(&dir.path().join("missing.dylib")).is_err());
    }
}
