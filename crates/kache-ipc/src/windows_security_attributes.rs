//! Decode only the ordinary process-identity attribute. Unknown token
//! attributes keep compiler execution local.

use std::io;
use std::mem::size_of;

// TokenSecurityAttributes returns the native TOKEN_SECURITY_ATTRIBUTES_INFORMATION
// layout, whose names are counted UNICODE_STRINGs, rather than the public
// CLAIM_SECURITY_ATTRIBUTES_INFORMATION layout with NUL-terminated names.
#[repr(C)]
#[derive(Clone, Copy)]
struct Attributes {
    version: u16,
    reserved: u16,
    count: u32,
    first: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct UnicodeString {
    length: u16,
    maximum_length: u16,
    buffer: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Attribute {
    name: UnicodeString,
    value_type: u16,
    reserved: u16,
    flags: u32,
    count: u32,
    values: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum OrdinaryAttributes {
    Empty,
    ProcessUnique,
}

pub(super) fn token_generations_match(
    same_generation: bool,
    has_opaque_attributes: bool,
    trusted_pipeline: bool,
    compare: impl FnOnce() -> io::Result<bool>,
) -> io::Result<bool> {
    if same_generation || !has_opaque_attributes {
        return Ok(true);
    }
    Ok(trusted_pipeline && compare()?)
}

pub(super) fn matching_ordinary_attributes(client: &[u8], daemon: &[u8]) -> io::Result<bool> {
    let Some(client) = ordinary_attributes(client)? else {
        return Ok(false);
    };
    Ok(Some(client) == ordinary_attributes(daemon)?)
}

fn ordinary_attributes(bytes: &[u8]) -> io::Result<Option<OrdinaryAttributes>> {
    let header: Attributes = value(bytes, 0)?;
    if header.version != 1 || header.reserved != 0 {
        return Ok(None);
    }
    match header.count {
        0 => Ok(Some(OrdinaryAttributes::Empty)),
        1 => {
            let attribute: Attribute = value(bytes, offset(bytes, header.first)?)?;
            // UINT64, non-inheritable + unique, two per-process values. An
            // unfamiliar shape must not gain the known nonce's exception.
            if attribute.value_type != 2
                || attribute.reserved != 0
                || attribute.flags != 0x41
                || attribute.count != 2
                || attribute.name.length != 32
                || attribute.name.maximum_length < attribute.name.length
            {
                return Ok(None);
            }
            let name = region(bytes, attribute.name.buffer, 32)?;
            let expected: Vec<u8> = "TSA://ProcUnique"
                .encode_utf16()
                .flat_map(u16::to_ne_bytes)
                .collect();
            if name != expected {
                return Ok(None);
            }
            // Validate the value pointer even though the values are identities,
            // and cannot be compared between independently created processes.
            region(bytes, attribute.values, 16)?;
            Ok(Some(OrdinaryAttributes::ProcessUnique))
        }
        _ => Ok(None),
    }
}

fn offset(bytes: &[u8], pointer: usize) -> io::Result<usize> {
    pointer
        .checked_sub(bytes.as_ptr() as usize)
        .ok_or_else(|| io::Error::other("invalid token security attribute pointer"))
}

fn region(bytes: &[u8], pointer: usize, length: usize) -> io::Result<&[u8]> {
    let start = offset(bytes, pointer)?;
    bytes
        .get(start..)
        .and_then(|tail| tail.get(..length))
        .ok_or_else(|| io::Error::other("truncated token security attribute"))
}

fn value<T: Copy>(bytes: &[u8], start: usize) -> io::Result<T> {
    let bytes = bytes
        .get(start..)
        .and_then(|tail| tail.get(..size_of::<T>()))
        .ok_or_else(|| io::Error::other("truncated token security attribute record"))?;
    // The caller chooses one of the integer-only native records above. The
    // returned token buffer need not align each nested record for Rust.
    Ok(unsafe { bytes.as_ptr().cast::<T>().read_unaligned() })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn different_generations_require_trust_and_matching_known_attributes() {
        for trusted in [false, true] {
            for matching in [false, true] {
                let expected = matches!((trusted, matching), (true, true));
                assert_eq!(
                    token_generations_match(false, true, trusted, || Ok(matching)).unwrap(),
                    expected
                );
            }
        }
        assert!(
            !token_generations_match(false, true, false, || {
                panic!("untrusted contexts must not query the reserved attribute class")
            })
            .unwrap()
        );
        assert!(
            token_generations_match(false, true, true, || {
                Err(io::Error::other("unavailable attributes"))
            })
            .is_err()
        );
    }

    #[test]
    fn unchanged_generations_and_tokens_without_attributes_need_no_exception() {
        for (same_generation, has_attributes) in [(true, true), (true, false), (false, false)] {
            for trusted in [false, true] {
                assert!(
                    token_generations_match(same_generation, has_attributes, trusted, || {
                        panic!("the original admission path must not read reserved attributes")
                    })
                    .unwrap()
                );
            }
        }
    }

    struct Fixture {
        bytes: Vec<u8>,
    }

    impl Fixture {
        fn new() -> Self {
            let attribute = size_of::<Attributes>();
            let name = attribute + size_of::<Attribute>();
            let values = name + 32;
            let mut fixture = Self {
                bytes: vec![0; values + 16],
            };
            let base = fixture.bytes.as_ptr() as usize;
            fixture.set(
                0,
                Attributes {
                    version: 1,
                    reserved: 0,
                    count: 1,
                    first: base + attribute,
                },
            );
            fixture.set(
                attribute,
                Attribute {
                    name: UnicodeString {
                        length: 32,
                        maximum_length: 32,
                        buffer: base + name,
                    },
                    value_type: 2,
                    reserved: 0,
                    flags: 0x41,
                    count: 2,
                    values: base + values,
                },
            );
            for (index, word) in "TSA://ProcUnique".encode_utf16().enumerate() {
                fixture.set(name + index * 2, word);
            }
            fixture.set(values, [12u64, 34u64]);
            fixture
        }

        fn set<T: Copy>(&mut self, offset: usize, record: T) {
            assert!(offset + size_of::<T>() <= self.bytes.len());
            unsafe {
                self.bytes
                    .as_mut_ptr()
                    .add(offset)
                    .cast::<T>()
                    .write_unaligned(record)
            };
        }

        fn header(&mut self, update: impl FnOnce(&mut Attributes)) {
            let mut header = value(&self.bytes, 0).unwrap();
            update(&mut header);
            self.set(0, header);
        }

        fn attribute(&mut self, update: impl FnOnce(&mut Attribute)) {
            let mut attribute = value(&self.bytes, size_of::<Attributes>()).unwrap();
            update(&mut attribute);
            self.set(size_of::<Attributes>(), attribute);
        }
    }

    #[test]
    fn only_matching_known_attribute_lists_can_share_a_pipeline() {
        let mut client = Fixture::new();
        let mut daemon = Fixture::new();
        daemon.set(daemon.bytes.len() - 16, [55u64, 66u64]);
        assert!(matching_ordinary_attributes(&client.bytes, &daemon.bytes).unwrap());
        daemon.header(|header| header.count = 0);
        assert!(!matching_ordinary_attributes(&client.bytes, &daemon.bytes).unwrap());
        assert!(!matching_ordinary_attributes(&daemon.bytes, &client.bytes).unwrap());
        client.header(|header| header.count = 0);
        assert!(matching_ordinary_attributes(&client.bytes, &daemon.bytes).unwrap());
        client.header(|header| header.count = 2);
        assert!(!matching_ordinary_attributes(&client.bytes, &daemon.bytes).unwrap());
        assert!(!matching_ordinary_attributes(&daemon.bytes, &client.bytes).unwrap());
        daemon.header(|header| header.count = 2);
        assert!(!matching_ordinary_attributes(&client.bytes, &daemon.bytes).unwrap());
    }

    #[test]
    fn recognizes_only_the_known_process_identity_shape() {
        let mut fixture = Fixture::new();
        assert_eq!(
            ordinary_attributes(&fixture.bytes).unwrap(),
            Some(OrdinaryAttributes::ProcessUnique)
        );
        fixture.set(fixture.bytes.len() - 16, [u64::MAX, 0u64]);
        assert_eq!(
            ordinary_attributes(&fixture.bytes).unwrap(),
            Some(OrdinaryAttributes::ProcessUnique)
        );
        fixture.header(|header| header.count = 0);
        assert_eq!(
            ordinary_attributes(&fixture.bytes).unwrap(),
            Some(OrdinaryAttributes::Empty)
        );
    }

    #[test]
    fn rejects_unknown_names_shapes_and_additional_attributes() {
        for change in [
            |a: &mut Attribute| a.value_type = 1,
            |a: &mut Attribute| a.reserved = 1,
            |a: &mut Attribute| a.flags = 1,
            |a: &mut Attribute| a.flags = 0x40,
            |a: &mut Attribute| a.flags = 0x61,
            |a: &mut Attribute| a.count = 1,
            |a: &mut Attribute| a.count = 3,
            |a: &mut Attribute| a.name.length = 30,
            |a: &mut Attribute| a.name.maximum_length = 30,
        ] {
            let mut fixture = Fixture::new();
            fixture.attribute(change);
            assert_eq!(ordinary_attributes(&fixture.bytes).unwrap(), None);
        }
        for change in [
            |h: &mut Attributes| h.version = 2,
            |h: &mut Attributes| h.reserved = 1,
            |h: &mut Attributes| h.count = 2,
        ] {
            let mut fixture = Fixture::new();
            fixture.header(change);
            assert_eq!(ordinary_attributes(&fixture.bytes).unwrap(), None);
        }
        let mut fixture = Fixture::new();
        let name = size_of::<Attributes>() + size_of::<Attribute>();
        fixture.set(name, u16::from(b'X'));
        assert_eq!(ordinary_attributes(&fixture.bytes).unwrap(), None);
    }

    #[test]
    fn malformed_buffers_and_pointers_are_errors() {
        for length in 0..size_of::<Attributes>() {
            assert!(ordinary_attributes(&vec![0; length]).is_err());
        }
        for pointer in [0, usize::MAX] {
            let mut fixture = Fixture::new();
            fixture.header(|header| header.first = pointer);
            assert!(ordinary_attributes(&fixture.bytes).is_err());
            let mut fixture = Fixture::new();
            fixture.attribute(|attribute| attribute.name.buffer = pointer);
            assert!(ordinary_attributes(&fixture.bytes).is_err());
            let mut fixture = Fixture::new();
            fixture.attribute(|attribute| attribute.values = pointer);
            assert!(ordinary_attributes(&fixture.bytes).is_err());
        }
        for remaining in [0, 1, size_of::<Attribute>() - 1] {
            let mut fixture = Fixture::new();
            let pointer = fixture.bytes.as_ptr() as usize + fixture.bytes.len() - remaining;
            fixture.header(|header| header.first = pointer);
            assert!(ordinary_attributes(&fixture.bytes).is_err());
        }
        for remaining in [0, 15] {
            let mut fixture = Fixture::new();
            let pointer = fixture.bytes.as_ptr() as usize + fixture.bytes.len() - remaining;
            fixture.attribute(|attribute| attribute.values = pointer);
            assert!(ordinary_attributes(&fixture.bytes).is_err());
        }
        for remaining in [0, 31] {
            let mut fixture = Fixture::new();
            let pointer = fixture.bytes.as_ptr() as usize + fixture.bytes.len() - remaining;
            fixture.attribute(|attribute| attribute.name.buffer = pointer);
            assert!(ordinary_attributes(&fixture.bytes).is_err());
        }
    }
}
