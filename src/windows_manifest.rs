//! The closed XML subset accepted for native MSVC /MANIFESTINPUT files.
//!
//! LINK/mt can process manifests with further file dependencies. Admit only
//! application metadata whose elements and attributes cannot name such inputs.
//! This is an admission check, not Microsoft's application-manifest validator.

use std::borrow::Cow;

use anyhow::{Context, Result, bail, ensure};
use quick_xml::events::{BytesStart, Event};
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader;

// Bounds apply to original bytes, nesting, elements, attributes per element,
// and namespace bindings in scope. The byte limit also bounds text and tokens.
pub(crate) const MAX_BYTES: usize = 1_048_576;
const MAX_DEPTH: usize = 32;
const MAX_ELEMENTS: usize = 4096;
const MAX_ATTRIBUTES: usize = 32;
const MAX_NAMESPACES: usize = 32;

const ASM1: &str = "urn:schemas-microsoft-com:asm.v1";
const ASM2: &str = "urn:schemas-microsoft-com:asm.v2";
const ASM3: &str = "urn:schemas-microsoft-com:asm.v3";
const COMPAT: &str = "urn:schemas-microsoft-com:compatibility.v1";
const SETTINGS_2005: &str = "http://schemas.microsoft.com/SMI/2005/WindowsSettings";
const SETTINGS_2016: &str = "http://schemas.microsoft.com/SMI/2016/WindowsSettings";

#[derive(Clone, Copy)]
enum Encoding {
    Utf8,
    Utf16Le,
    Utf16Be,
}

fn decode(bytes: &[u8]) -> Result<(Cow<'_, str>, Encoding)> {
    let (body, encoding) = if let Some(body) = bytes.strip_prefix(&[0xff, 0xfe]) {
        (body, Encoding::Utf16Le)
    } else if let Some(body) = bytes.strip_prefix(&[0xfe, 0xff]) {
        (body, Encoding::Utf16Be)
    } else {
        let body = bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes);
        return Ok((std::str::from_utf8(body)?.into(), Encoding::Utf8));
    };
    ensure!(body.len().is_multiple_of(2), "truncated UTF-16 manifest");
    let words: Vec<u16> = body
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| match encoding {
            Encoding::Utf16Le => u16::from_le_bytes([pair[0], pair[1]]),
            Encoding::Utf16Be => u16::from_be_bytes([pair[0], pair[1]]),
            Encoding::Utf8 => unreachable!(),
        })
        .collect();
    Ok((String::from_utf16(&words)?.into(), encoding))
}

fn xml_character(c: char) -> bool {
    matches!(c, '\t' | '\n' | '\r' | '\u{20}'..='\u{d7ff}' | '\u{e000}'..='\u{fffd}' | '\u{10000}'..='\u{10ffff}')
}

// Restrict names to ASCII QNames. All admitted expanded names are ASCII;
// excluding other prefix spellings also avoids accepting malformed XML names.
fn check_name(name: &str) -> Result<()> {
    let mut parts = name.split(':');
    for part in parts.by_ref().take(2) {
        let mut chars = part.chars();
        ensure!(
            chars
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.')),
            "unsupported XML name"
        );
    }
    ensure!(parts.next().is_none(), "unsupported XML QName");
    Ok(())
}

fn check_reference(reference: &str) -> Result<()> {
    if matches!(reference, "amp" | "lt" | "gt" | "apos" | "quot") {
        return Ok(());
    }
    let (number, radix) = if let Some(number) = reference.strip_prefix("#x") {
        (number, 16)
    } else if let Some(number) = reference.strip_prefix('#') {
        (number, 10)
    } else {
        bail!("general entities are not supported in cached manifests");
    };
    ensure!(
        !number.is_empty() && number.chars().all(|c| c.is_digit(radix)),
        "invalid XML character reference"
    );
    let c = u32::from_str_radix(number, radix)
        .ok()
        .and_then(char::from_u32)
        .context("invalid XML character reference")?;
    ensure!(xml_character(c), "invalid XML character reference");
    Ok(())
}

fn check_attribute_value(mut value: &str) -> Result<()> {
    ensure!(!value.contains('<'), "invalid XML attribute value");
    while let Some((_, rest)) = value.split_once('&') {
        let (reference, tail) = rest
            .split_once(';')
            .context("unclosed XML character reference")?;
        check_reference(reference)?;
        value = tail;
    }
    Ok(())
}

// quick-xml checks quotes and duplicate attributes, but accepts `a="x"b="y"`.
// XML requires whitespace after a quoted attribute before another attribute.
fn check_attribute_separators(raw: &str) -> Result<()> {
    let bytes = raw.as_bytes();
    let mut quote = None;
    for (index, &byte) in bytes.iter().enumerate() {
        if quote == Some(byte) {
            quote = None;
            ensure!(
                bytes
                    .get(index + 1)
                    .is_none_or(|b| matches!(b, b' ' | b'\t' | b'\r' | b'\n')),
                "missing whitespace between XML attributes"
            );
        } else if quote.is_none() && matches!(byte, b'\'' | b'"') {
            quote = Some(byte);
        }
    }
    Ok(())
}

fn known_namespace(namespace: &str) -> bool {
    matches!(
        namespace,
        ASM1 | ASM2 | ASM3 | COMPAT | SETTINGS_2005 | SETTINGS_2016
    )
}

fn attributes(namespace: &str, name: &str) -> Option<&'static [&'static str]> {
    match (namespace, name) {
        (ASM1, "assembly") => Some(&["manifestVersion"]),
        (ASM1, "assemblyIdentity") => Some(&[
            "name",
            "version",
            "type",
            "processorArchitecture",
            "publicKeyToken",
            "language",
        ]),
        (ASM1, "dependency") => Some(&["optional"]),
        (ASM1, "description" | "dependentAssembly" | "noInherit")
        | (ASM2 | ASM3, "trustInfo" | "security" | "requestedPrivileges")
        | (COMPAT, "compatibility" | "application")
        | (ASM3, "application" | "windowsSettings")
        | (SETTINGS_2005, "dpiAware")
        | (SETTINGS_2016, "dpiAwareness" | "longPathAware") => Some(&[]),
        (ASM2 | ASM3, "requestedExecutionLevel") => Some(&["level", "uiAccess"]),
        (COMPAT, "supportedOS" | "maxversiontested") => Some(&["Id"]),
        _ => None,
    }
}

fn check_element(reader: &NsReader<&[u8]>, element: &BytesStart<'_>, root: bool) -> Result<()> {
    check_attribute_separators(element.attributes_raw())?;
    check_name(element.name().into_inner())?;
    let (namespace, local) = reader.resolver().resolve_element(element.name());
    let ResolveResult::Bound(namespace) = namespace else {
        bail!("manifest element has no supported namespace");
    };
    let namespace = namespace.into_inner();
    let local = local.into_inner();
    ensure!(
        !root || (namespace == ASM1 && local == "assembly"),
        "manifest root must be asm.v1 assembly"
    );
    let allowed = attributes(namespace, local).context("unsupported manifest element")?;
    for (index, attribute) in element.attributes().enumerate() {
        ensure!(index < MAX_ATTRIBUTES, "too many manifest attributes");
        let attribute = attribute?;
        let name = attribute.key.into_inner();
        check_name(name)?;
        check_attribute_value(&attribute.value)?;
        if name == "xmlns" || name.starts_with("xmlns:") {
            ensure!(
                known_namespace(&attribute.value),
                "unsupported manifest namespace"
            );
            continue;
        }
        let (namespace, local) = reader.resolver().resolve_attribute(attribute.key);
        ensure!(
            matches!(namespace, ResolveResult::Unbound) && allowed.contains(&local.into_inner()),
            "unsupported manifest attribute"
        );
    }
    Ok(())
}

fn check_declaration(declaration: &str, encoding: Encoding) -> Result<()> {
    let start = BytesStart::from_content(declaration, 3);
    check_attribute_separators(start.attributes_raw())?;
    let mut attrs = start.attributes();
    let version = attrs.next().context("missing XML version")??;
    ensure!(
        version.key.into_inner() == "version" && version.value == "1.0",
        "unsupported XML version"
    );
    let mut saw_standalone = false;
    for attribute in attrs {
        let attribute = attribute?;
        match attribute.key.into_inner() {
            "encoding" if !saw_standalone => {
                let value = attribute.value.as_ref();
                let matches = match encoding {
                    Encoding::Utf8 => value.eq_ignore_ascii_case("UTF-8"),
                    Encoding::Utf16Le => {
                        value.eq_ignore_ascii_case("UTF-16")
                            || value.eq_ignore_ascii_case("UTF-16LE")
                    }
                    Encoding::Utf16Be => {
                        value.eq_ignore_ascii_case("UTF-16")
                            || value.eq_ignore_ascii_case("UTF-16BE")
                    }
                };
                ensure!(
                    matches,
                    "manifest encoding declaration disagrees with bytes"
                );
            }
            "standalone" => {
                saw_standalone = true;
                ensure!(
                    matches!(attribute.value.as_ref(), "yes" | "no"),
                    "invalid XML standalone declaration"
                );
            }
            _ => bail!("unsupported XML declaration"),
        }
    }
    Ok(())
}

pub(crate) fn validate(bytes: &[u8]) -> Result<()> {
    ensure!(bytes.len() <= MAX_BYTES, "manifest exceeds 1 MiB");
    let (text, encoding) = decode(bytes).context("decoding manifest")?;
    ensure!(text.chars().all(xml_character), "invalid XML character");
    let mut reader = NsReader::from_str(&text);
    reader.config_mut().check_comments = true;
    reader
        .resolver_mut()
        .set_max_namespace_bindings(MAX_NAMESPACES);
    let mut depth = 0usize;
    let mut elements = 0usize;
    let mut saw_event = false;
    loop {
        let event = reader.read_event().context("reading manifest XML")?;
        match &event {
            Event::Start(element) | Event::Empty(element) => {
                ensure!(depth < MAX_DEPTH, "manifest nesting exceeds 32 elements");
                ensure!(elements < MAX_ELEMENTS, "manifest exceeds 4096 elements");
                ensure!(depth != 0 || elements == 0, "multiple manifest roots");
                check_element(&reader, element, depth == 0)?;
                elements += 1;
                if matches!(event, Event::Start(_)) {
                    depth += 1;
                }
            }
            Event::End(_) => {
                depth = depth.checked_sub(1).context("unmatched manifest end tag")?;
            }
            Event::Decl(declaration) => {
                ensure!(!saw_event, "misplaced XML declaration");
                check_declaration(declaration, encoding)?;
            }
            Event::Text(text) => {
                ensure!(!text.contains("]]>"), "invalid XML text");
                ensure!(
                    depth != 0 || text.chars().all(|c| matches!(c, ' ' | '\t' | '\r' | '\n')),
                    "text outside manifest root"
                );
            }
            Event::GeneralRef(reference) => {
                ensure!(depth != 0, "reference outside manifest root");
                check_reference(reference)?;
            }
            Event::CData(_) => ensure!(depth != 0, "CDATA outside manifest root"),
            Event::Comment(_) => {}
            Event::Eof => {
                ensure!(elements != 0 && depth == 0, "incomplete manifest document");
                return Ok(());
            }
            Event::DocType(_) | Event::PI(_) => {
                bail!("DTD and processing instructions are not supported in cached manifests")
            }
        }
        saw_event = true;
    }
}

#[cfg(test)]
mod tests;
