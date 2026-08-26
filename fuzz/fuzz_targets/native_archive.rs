#![no_main]

use libfuzzer_sys::fuzz_target;

#[allow(dead_code)]
#[path = "../../src/checked_regions.rs"]
mod checked_regions;
#[path = "../../src/native_archive.rs"]
mod native_archive;

const MAX_INPUT_BYTES: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    let first = native_archive::portable_static_archive_hash(data);
    let second = native_archive::portable_static_archive_hash(data);
    assert_eq!(
        first, second,
        "native archive hashing must be deterministic"
    );

    if let Some(hash) = first {
        let digest = hash
            .strip_prefix("gnu-ar-v2:")
            .or_else(|| hash.strip_prefix("bsd-ar-v2:"))
            .expect("portable archive hash has a known domain tag");
        assert_eq!(digest.len(), 64, "portable archive hash is BLAKE3-sized");
        assert!(
            digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "portable archive digest is lowercase hexadecimal"
        );
    }
});
