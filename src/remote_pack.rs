//! Immutable multi-entry transport packs for speculative remote prefetch.
//!
//! A pack is a thin frame around existing v3 entry-pack payloads, not another
//! compression layer. That keeps byte overhead bounded and lets the importer
//! reuse established per-entry zstd/tar validation:
//!
//! `KACHPK01 | index_len: u64 LE | PackIndex JSON | v3 payloads...`
//!
//! Offsets are relative to the payload section, avoiding a circular dependency
//! between JSON length and encoded offsets.

#![allow(dead_code)] // foundations consumed by publisher/discovery slices

use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub(crate) const PACK_VERSION: u32 = 1;
pub(crate) const CATALOG_VERSION: u32 = 1;
pub(crate) const DEFAULT_MAX_PACK_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const PACK_CONTENT_TYPE: &str = "application/vnd.kache.prefetch-pack.v1";
pub(crate) const CATALOG_CONTENT_TYPE: &str = "application/vnd.kache.prefetch-catalog.v1+json";

const PACK_MAGIC: &[u8; 8] = b"KACHPK01";
const PACK_HEADER_BYTES: usize = PACK_MAGIC.len() + size_of::<u64>();
const MAX_PACK_INDEX_BYTES: usize = 16 * 1024 * 1024;
const MAX_CATALOG_BYTES: usize = 64 * 1024 * 1024;
const MAX_PACK_ENTRIES: usize = 65_536;
const MAX_CATALOG_PACKS: usize = 16_384;
const MAX_SELECTOR_TEXT_BYTES: usize = 4096;

#[derive(Debug, Clone)]
pub(crate) struct PackInputEntry {
    pub cache_key: String,
    pub crate_name: String,
    /// BLAKE3 of the exact `meta.json` carried by `payload`.
    pub meta_digest: String,
    /// Existing v3 tar.zst entry-pack bytes.
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PackIndexEntry {
    pub cache_key: String,
    pub crate_name: String,
    pub meta_digest: String,
    /// Byte offset relative to the first payload byte.
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PackIndex {
    pub version: u32,
    pub key_schema: u32,
    pub entries: Vec<PackIndexEntry>,
}

#[derive(Debug, Clone)]
pub(crate) struct BuiltPack {
    pub bytes: Vec<u8>,
    pub digest: String,
    pub object_key: String,
    pub index: PackIndex,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedPackEntry<'a> {
    pub descriptor: PackIndexEntry,
    pub payload: &'a [u8],
}

#[derive(Debug)]
pub(crate) struct DecodedPack<'a> {
    pub index: PackIndex,
    pub entries: Vec<DecodedPackEntry<'a>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatalogEntry {
    pub cache_key: String,
    pub crate_name: String,
    pub meta_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatalogPackRef {
    pub digest: String,
    pub pack_bytes: u64,
    pub entries: Vec<CatalogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PackCatalog {
    pub version: u32,
    pub key_schema: u32,
    pub manifest_key: String,
    pub namespace: String,
    pub selector_hash: String,
    pub shard_hashes: Vec<String>,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    pub packs: Vec<CatalogPackRef>,
    /// Entries deliberately left on v3, for example one entry larger than the
    /// pack cap. Discovery schedules these through the compatibility path.
    pub fallback_entries: Vec<CatalogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PackMeta {
    pub version: u32,
    pub digest: String,
    pub pack_bytes: u64,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct EncodedCatalog {
    pub bytes: Vec<u8>,
    pub digest: String,
    pub object_key: String,
    pub catalog: PackCatalog,
}

/// Stable selector for one manifest/build namespace and Cargo.lock shard set.
/// Raw user-controlled text is length-prefixed into BLAKE3 and never used as an
/// object-key path component.
pub(crate) fn selector_hash(
    manifest_key: &str,
    namespace: &str,
    shard_hashes: &[String],
    key_schema: u32,
) -> Result<String> {
    validate_selector_text("manifest key", manifest_key)?;
    validate_selector_text("namespace", namespace)?;
    let mut shards = shard_hashes.to_vec();
    shards.sort();
    if shards.windows(2).any(|pair| pair[0] == pair[1]) {
        bail!("selector contains duplicate Cargo.lock shard hashes");
    }
    for shard in &shards {
        validate_digest("Cargo.lock shard hash", shard)?;
    }

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"kache-prefetch-selector-v1");
    hash_field(&mut hasher, manifest_key.as_bytes());
    hash_field(&mut hasher, namespace.as_bytes());
    hasher.update(&key_schema.to_le_bytes());
    hasher.update(&(shards.len() as u64).to_le_bytes());
    for shard in &shards {
        hash_field(&mut hasher, shard.as_bytes());
    }
    Ok(hasher.finalize().to_hex().to_string())
}

pub(crate) fn pack_object_key(prefix: &str, digest: &str) -> Result<String> {
    validate_digest("pack digest", digest)?;
    Ok(crate::config::join_remote_key(
        prefix,
        &format!("v4/prefetch/packs/{}/{}.kpack", &digest[..2], digest),
    ))
}

pub(crate) fn pack_meta_object_key(prefix: &str, digest: &str) -> Result<String> {
    validate_digest("pack digest", digest)?;
    Ok(crate::config::join_remote_key(
        prefix,
        &format!("v4/prefetch/pack-meta/{digest}.json"),
    ))
}

pub(crate) fn catalog_prefix(prefix: &str, selector: &str) -> Result<String> {
    validate_digest("catalog selector", selector)?;
    Ok(crate::config::join_remote_key(
        prefix,
        &format!("v4/prefetch/catalogs/{selector}/"),
    ))
}

pub(crate) fn catalog_object_key(
    prefix: &str,
    selector: &str,
    created_at_ms: u64,
    digest: &str,
) -> Result<String> {
    validate_digest("catalog selector", selector)?;
    validate_digest("catalog digest", digest)?;
    Ok(crate::config::join_remote_key(
        prefix,
        &format!("v4/prefetch/catalogs/{selector}/{created_at_ms:020}-{digest}.json"),
    ))
}

pub(crate) fn build_pack(
    prefix: &str,
    mut entries: Vec<PackInputEntry>,
    max_bytes: u64,
) -> Result<BuiltPack> {
    validate_pack_cap(max_bytes)?;
    if entries.is_empty() {
        bail!("cannot build an empty prefetch pack");
    }
    if entries.len() > MAX_PACK_ENTRIES {
        bail!("prefetch pack has too many entries: {}", entries.len());
    }
    entries.sort_by(|left, right| {
        (&left.cache_key, &left.crate_name).cmp(&(&right.cache_key, &right.crate_name))
    });

    let mut index_entries = Vec::with_capacity(entries.len());
    let mut next_offset = 0_u64;
    let mut previous_key: Option<&str> = None;
    for entry in &entries {
        validate_entry_fields(&entry.cache_key, &entry.crate_name, &entry.meta_digest)?;
        if previous_key == Some(entry.cache_key.as_str()) {
            bail!(
                "prefetch pack contains duplicate cache key {}",
                entry.cache_key
            );
        }
        previous_key = Some(&entry.cache_key);
        if entry.payload.is_empty() {
            bail!(
                "prefetch pack entry {} has an empty v3 payload",
                entry.cache_key
            );
        }
        let length = u64::try_from(entry.payload.len()).context("entry payload length overflow")?;
        index_entries.push(PackIndexEntry {
            cache_key: entry.cache_key.clone(),
            crate_name: entry.crate_name.clone(),
            meta_digest: entry.meta_digest.clone(),
            offset: next_offset,
            length,
        });
        next_offset = next_offset
            .checked_add(length)
            .context("prefetch pack payload length overflow")?;
    }

    let index = PackIndex {
        version: PACK_VERSION,
        key_schema: crate::cache_key::CACHE_KEY_VERSION,
        entries: index_entries,
    };
    let index_json = serde_json::to_vec(&index).context("serializing prefetch pack index")?;
    if index_json.len() > MAX_PACK_INDEX_BYTES {
        bail!("prefetch pack index exceeds {MAX_PACK_INDEX_BYTES} bytes");
    }
    let payload_len = usize::try_from(next_offset).context("prefetch payload length overflow")?;
    let total_len = PACK_HEADER_BYTES
        .checked_add(index_json.len())
        .and_then(|len| len.checked_add(payload_len))
        .context("prefetch pack length overflow")?;
    if total_len as u64 > max_bytes {
        bail!("prefetch pack is {total_len} bytes, above the {max_bytes}-byte cap");
    }

    let mut bytes = Vec::with_capacity(total_len);
    bytes.extend_from_slice(PACK_MAGIC);
    bytes.extend_from_slice(&(index_json.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&index_json);
    for entry in entries {
        bytes.extend_from_slice(&entry.payload);
    }
    debug_assert_eq!(bytes.len(), total_len);
    let digest = blake3::hash(&bytes).to_hex().to_string();
    let object_key = pack_object_key(prefix, &digest)?;
    Ok(BuiltPack {
        bytes,
        digest,
        object_key,
        index,
    })
}

pub(crate) fn decode_pack<'a>(
    bytes: &'a [u8],
    expected_digest: &str,
    max_bytes: u64,
) -> Result<DecodedPack<'a>> {
    validate_pack_cap(max_bytes)?;
    validate_digest("expected pack digest", expected_digest)?;
    if bytes.len() as u64 > max_bytes {
        bail!(
            "prefetch pack is {} bytes, above the {max_bytes}-byte cap",
            bytes.len()
        );
    }
    let actual_digest = blake3::hash(bytes).to_hex().to_string();
    if actual_digest != expected_digest {
        bail!("prefetch pack digest mismatch (expected {expected_digest}, got {actual_digest})");
    }
    if bytes.len() < PACK_HEADER_BYTES || &bytes[..PACK_MAGIC.len()] != PACK_MAGIC {
        bail!("invalid prefetch pack magic or truncated header");
    }
    let index_len = u64::from_le_bytes(
        bytes[PACK_MAGIC.len()..PACK_HEADER_BYTES]
            .try_into()
            .expect("fixed-size header slice"),
    );
    let index_len = usize::try_from(index_len).context("prefetch pack index length overflow")?;
    if index_len > MAX_PACK_INDEX_BYTES {
        bail!("prefetch pack index exceeds {MAX_PACK_INDEX_BYTES} bytes");
    }
    let payload_start = PACK_HEADER_BYTES
        .checked_add(index_len)
        .context("prefetch pack index length overflow")?;
    if payload_start > bytes.len() {
        bail!("prefetch pack index length exceeds object length");
    }
    let index: PackIndex = serde_json::from_slice(&bytes[PACK_HEADER_BYTES..payload_start])
        .context("parsing prefetch pack index")?;
    let payload = &bytes[payload_start..];
    validate_pack_index(&index, payload.len())?;

    let entries = index
        .entries
        .iter()
        .map(|entry| {
            let start = usize::try_from(entry.offset).expect("validated offset fits usize");
            let end = usize::try_from(entry.offset + entry.length)
                .expect("validated payload end fits usize");
            DecodedPackEntry {
                descriptor: entry.clone(),
                payload: &payload[start..end],
            }
        })
        .collect();
    Ok(DecodedPack { index, entries })
}

pub(crate) fn encode_catalog(prefix: &str, catalog: PackCatalog) -> Result<EncodedCatalog> {
    let catalog = canonicalize_catalog(catalog)?;
    let bytes = serde_json::to_vec(&catalog).context("serializing prefetch pack catalog")?;
    if bytes.len() > MAX_CATALOG_BYTES {
        bail!("prefetch pack catalog exceeds {MAX_CATALOG_BYTES} bytes");
    }
    let digest = blake3::hash(&bytes).to_hex().to_string();
    let object_key = catalog_object_key(
        prefix,
        &catalog.selector_hash,
        catalog.created_at_ms,
        &digest,
    )?;
    Ok(EncodedCatalog {
        bytes,
        digest,
        object_key,
        catalog,
    })
}

pub(crate) fn decode_catalog(bytes: &[u8], expected_digest: &str) -> Result<PackCatalog> {
    validate_digest("expected catalog digest", expected_digest)?;
    if bytes.len() > MAX_CATALOG_BYTES {
        bail!("prefetch pack catalog exceeds {MAX_CATALOG_BYTES} bytes");
    }
    let actual_digest = blake3::hash(bytes).to_hex().to_string();
    if actual_digest != expected_digest {
        bail!("prefetch catalog digest mismatch (expected {expected_digest}, got {actual_digest})");
    }
    let catalog: PackCatalog =
        serde_json::from_slice(bytes).context("parsing prefetch pack catalog")?;
    let canonical = canonicalize_catalog(catalog.clone())?;
    if canonical != catalog {
        bail!("prefetch pack catalog is not canonically ordered");
    }
    Ok(catalog)
}

fn canonicalize_catalog(mut catalog: PackCatalog) -> Result<PackCatalog> {
    catalog.shard_hashes.sort();
    for pack in &mut catalog.packs {
        pack.entries.sort_by(|left, right| {
            (&left.cache_key, &left.crate_name).cmp(&(&right.cache_key, &right.crate_name))
        });
    }
    catalog
        .packs
        .sort_by(|left, right| left.digest.cmp(&right.digest));
    catalog.fallback_entries.sort_by(|left, right| {
        (&left.cache_key, &left.crate_name).cmp(&(&right.cache_key, &right.crate_name))
    });
    validate_catalog(&catalog)?;
    Ok(catalog)
}

fn validate_catalog(catalog: &PackCatalog) -> Result<()> {
    if catalog.version != CATALOG_VERSION {
        bail!("unsupported prefetch catalog version {}", catalog.version);
    }
    if catalog.key_schema != crate::cache_key::CACHE_KEY_VERSION {
        bail!(
            "prefetch catalog key schema {} does not match current recipe {}",
            catalog.key_schema,
            crate::cache_key::CACHE_KEY_VERSION
        );
    }
    validate_selector_text("manifest key", &catalog.manifest_key)?;
    validate_selector_text("namespace", &catalog.namespace)?;
    if catalog.created_at_ms >= catalog.expires_at_ms {
        bail!("prefetch catalog expiry must be after creation");
    }
    if catalog
        .shard_hashes
        .windows(2)
        .any(|pair| pair[0] == pair[1])
    {
        bail!("prefetch catalog contains duplicate Cargo.lock shard hashes");
    }
    for shard in &catalog.shard_hashes {
        validate_digest("Cargo.lock shard hash", shard)?;
    }
    let expected_selector = selector_hash(
        &catalog.manifest_key,
        &catalog.namespace,
        &catalog.shard_hashes,
        catalog.key_schema,
    )?;
    if catalog.selector_hash != expected_selector {
        bail!(
            "prefetch catalog selector mismatch (expected {expected_selector}, got {})",
            catalog.selector_hash
        );
    }
    if catalog.packs.len() > MAX_CATALOG_PACKS {
        bail!(
            "prefetch catalog has too many packs: {}",
            catalog.packs.len()
        );
    }

    let mut pack_digests = HashSet::new();
    let mut cache_keys = HashSet::new();
    let mut total_entries = 0usize;
    for pack in &catalog.packs {
        validate_digest("pack digest", &pack.digest)?;
        if !pack_digests.insert(&pack.digest) {
            bail!("prefetch catalog contains duplicate pack {}", pack.digest);
        }
        if pack.pack_bytes == 0 || pack.pack_bytes > DEFAULT_MAX_PACK_BYTES {
            bail!(
                "prefetch catalog pack {} has invalid size {}",
                pack.digest,
                pack.pack_bytes
            );
        }
        if pack.entries.is_empty() || pack.entries.len() > MAX_PACK_ENTRIES {
            bail!(
                "prefetch catalog pack {} has an invalid entry count",
                pack.digest
            );
        }
        for entry in &pack.entries {
            validate_entry(entry)?;
            if !cache_keys.insert(&entry.cache_key) {
                bail!(
                    "prefetch catalog contains duplicate cache key {}",
                    entry.cache_key
                );
            }
        }
        total_entries = total_entries
            .checked_add(pack.entries.len())
            .context("prefetch catalog entry count overflow")?;
    }
    for entry in &catalog.fallback_entries {
        validate_entry(entry)?;
        if !cache_keys.insert(&entry.cache_key) {
            bail!(
                "prefetch catalog contains duplicate cache key {}",
                entry.cache_key
            );
        }
    }
    total_entries = total_entries
        .checked_add(catalog.fallback_entries.len())
        .context("prefetch catalog entry count overflow")?;
    if total_entries > MAX_PACK_ENTRIES {
        bail!("prefetch catalog has too many entries: {total_entries}");
    }
    Ok(())
}

fn validate_pack_index(index: &PackIndex, payload_len: usize) -> Result<()> {
    if index.version != PACK_VERSION {
        bail!("unsupported prefetch pack version {}", index.version);
    }
    if index.key_schema != crate::cache_key::CACHE_KEY_VERSION {
        bail!(
            "prefetch pack key schema {} does not match current recipe {}",
            index.key_schema,
            crate::cache_key::CACHE_KEY_VERSION
        );
    }
    if index.entries.is_empty() || index.entries.len() > MAX_PACK_ENTRIES {
        bail!("prefetch pack has an invalid entry count");
    }
    let mut expected_offset = 0_u64;
    let mut previous_key: Option<&str> = None;
    for entry in &index.entries {
        validate_entry_fields(&entry.cache_key, &entry.crate_name, &entry.meta_digest)?;
        if previous_key.is_some_and(|previous| previous >= entry.cache_key.as_str()) {
            bail!("prefetch pack entries are duplicated or not canonically ordered");
        }
        previous_key = Some(&entry.cache_key);
        if entry.offset != expected_offset || entry.length == 0 {
            bail!("prefetch pack entry frames overlap, contain a gap, or are empty");
        }
        expected_offset = expected_offset
            .checked_add(entry.length)
            .context("prefetch pack frame length overflow")?;
    }
    if expected_offset != payload_len as u64 {
        bail!(
            "prefetch pack frame lengths total {expected_offset} bytes, object contains {payload_len}"
        );
    }
    Ok(())
}

fn validate_pack_cap(max_bytes: u64) -> Result<()> {
    if max_bytes == 0 || max_bytes > DEFAULT_MAX_PACK_BYTES {
        bail!(
            "prefetch pack cap must be between 1 and {DEFAULT_MAX_PACK_BYTES} bytes, got {max_bytes}"
        );
    }
    Ok(())
}

fn validate_entry(entry: &CatalogEntry) -> Result<()> {
    validate_entry_fields(&entry.cache_key, &entry.crate_name, &entry.meta_digest)
}

fn validate_entry_fields(cache_key: &str, crate_name: &str, meta_digest: &str) -> Result<()> {
    if !crate::cache_key::is_valid_cache_key(cache_key) {
        bail!("prefetch metadata contains invalid cache key {cache_key:?}");
    }
    if !crate::cache_key::is_valid_crate_name(crate_name) {
        bail!("prefetch metadata contains unsafe crate name {crate_name:?}");
    }
    validate_digest("entry metadata digest", meta_digest)
}

fn validate_digest(label: &str, value: &str) -> Result<()> {
    if !crate::cache_key::is_valid_cache_key(value) {
        bail!("{label} is not a lowercase BLAKE3 digest: {value:?}");
    }
    Ok(())
}

fn validate_selector_text(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_SELECTOR_TEXT_BYTES
        || value.chars().any(char::is_control)
    {
        bail!("prefetch {label} is empty, too long, or contains control characters");
    }
    Ok(())
}

fn hash_field(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(label: &str) -> String {
        blake3::hash(label.as_bytes()).to_hex().to_string()
    }

    fn input(label: &str, crate_name: &str, payload: &[u8]) -> PackInputEntry {
        PackInputEntry {
            cache_key: digest(label),
            crate_name: crate_name.to_string(),
            meta_digest: digest(&format!("meta-{label}")),
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn pack_encoding_is_deterministic_and_round_trips() {
        let one = input("one", "serde", b"first-v3-pack");
        let two = input("two", "tokio", b"second-v3-pack");

        let forward = build_pack("artifacts", vec![one.clone(), two.clone()], 4096).unwrap();
        let reversed = build_pack("artifacts", vec![two, one], 4096).unwrap();
        assert_eq!(forward.bytes, reversed.bytes);
        assert_eq!(forward.digest, reversed.digest);
        assert_eq!(
            forward.object_key,
            format!(
                "artifacts/v4/prefetch/packs/{}/{}.kpack",
                &forward.digest[..2],
                forward.digest
            )
        );

        let decoded = decode_pack(&forward.bytes, &forward.digest, 4096).unwrap();
        assert_eq!(decoded.entries.len(), 2);
        let by_key = decoded
            .entries
            .iter()
            .map(|entry| (entry.descriptor.cache_key.as_str(), entry.payload))
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            by_key.get(digest("one").as_str()).copied(),
            Some(b"first-v3-pack".as_slice())
        );
        assert_eq!(
            by_key.get(digest("two").as_str()).copied(),
            Some(b"second-v3-pack".as_slice())
        );
    }

    #[test]
    fn pack_rejects_duplicates_traversal_digest_length_and_cap_violations() {
        let one = input("one", "serde", b"first");
        assert!(build_pack("", vec![one.clone(), one.clone()], 4096).is_err());

        let mut traversal = one.clone();
        traversal.crate_name = "../escape".to_string();
        assert!(build_pack("", vec![traversal], 4096).is_err());

        let built = build_pack("", vec![one], 4096).unwrap();
        assert!(build_pack("", vec![input("tiny-cap", "serde", b"x")], 1).is_err());
        assert!(decode_pack(&built.bytes, &digest("wrong"), 4096).is_err());
        assert!(decode_pack(&built.bytes, &built.digest, 1).is_err());

        let mut truncated = built.bytes.clone();
        truncated.pop();
        let truncated_digest = blake3::hash(&truncated).to_hex().to_string();
        assert!(decode_pack(&truncated, &truncated_digest, 4096).is_err());

        let mut gap = built.bytes.clone();
        let index_len = u64::from_le_bytes(gap[8..16].try_into().unwrap()) as usize;
        let mut index: PackIndex = serde_json::from_slice(&gap[16..16 + index_len]).unwrap();
        index.entries[0].offset = 1;
        let changed = serde_json::to_vec(&index).unwrap();
        assert_eq!(
            changed.len(),
            index_len,
            "fixture must preserve index framing"
        );
        gap[16..16 + index_len].copy_from_slice(&changed);
        let gap_digest = blake3::hash(&gap).to_hex().to_string();
        assert!(decode_pack(&gap, &gap_digest, 4096).is_err());

        let mut wrong_recipe = built.bytes.clone();
        let index_len = u64::from_le_bytes(wrong_recipe[8..16].try_into().unwrap()) as usize;
        let mut index: PackIndex =
            serde_json::from_slice(&wrong_recipe[16..16 + index_len]).unwrap();
        index.key_schema -= 1;
        let changed = serde_json::to_vec(&index).unwrap();
        assert_eq!(changed.len(), index_len);
        wrong_recipe[16..16 + index_len].copy_from_slice(&changed);
        let wrong_recipe_digest = blake3::hash(&wrong_recipe).to_hex().to_string();
        assert!(decode_pack(&wrong_recipe, &wrong_recipe_digest, 4096).is_err());
    }

    #[test]
    fn object_keys_accept_only_digests_not_path_components() {
        assert!(pack_object_key("", "../escape").is_err());
        assert!(pack_meta_object_key("", &"A".repeat(64)).is_err());
        assert!(catalog_prefix("", "a/b").is_err());
    }

    #[test]
    fn selector_and_catalog_are_order_independent_and_content_addressed() {
        let shard_a = digest("shard-a");
        let shard_b = digest("shard-b");
        let selector_a = selector_hash(
            "x86_64-unknown-linux-gnu",
            "trusted/linux/release",
            &[shard_a.clone(), shard_b.clone()],
            crate::cache_key::CACHE_KEY_VERSION,
        )
        .unwrap();
        let selector_b = selector_hash(
            "x86_64-unknown-linux-gnu",
            "trusted/linux/release",
            &[shard_b, shard_a],
            crate::cache_key::CACHE_KEY_VERSION,
        )
        .unwrap();
        assert_eq!(selector_a, selector_b);

        let entry = CatalogEntry {
            cache_key: digest("one"),
            crate_name: "serde".to_string(),
            meta_digest: digest("meta-one"),
        };
        let catalog = PackCatalog {
            version: CATALOG_VERSION,
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            manifest_key: "x86_64-unknown-linux-gnu".to_string(),
            namespace: "trusted/linux/release".to_string(),
            selector_hash: selector_a.clone(),
            shard_hashes: vec![digest("shard-b"), digest("shard-a")],
            created_at_ms: 100,
            expires_at_ms: 200,
            packs: vec![CatalogPackRef {
                digest: digest("pack"),
                pack_bytes: 123,
                entries: vec![entry],
            }],
            fallback_entries: Vec::new(),
        };
        let encoded = encode_catalog("artifacts", catalog).unwrap();
        let mut sorted_shards = vec![digest("shard-a"), digest("shard-b")];
        sorted_shards.sort();
        assert_eq!(encoded.catalog.shard_hashes, sorted_shards);
        assert_eq!(
            encoded.object_key,
            format!(
                "artifacts/v4/prefetch/catalogs/{}/{:020}-{}.json",
                selector_a, 100, encoded.digest
            )
        );
        assert_eq!(
            decode_catalog(&encoded.bytes, &encoded.digest).unwrap(),
            encoded.catalog
        );
    }

    #[test]
    fn catalog_rejects_duplicate_or_unsafe_entries() {
        let shard = digest("shard");
        let selector = selector_hash(
            "target",
            "namespace",
            std::slice::from_ref(&shard),
            crate::cache_key::CACHE_KEY_VERSION,
        )
        .unwrap();
        let entry = CatalogEntry {
            cache_key: digest("one"),
            crate_name: "serde".to_string(),
            meta_digest: digest("meta-one"),
        };
        let catalog = PackCatalog {
            version: CATALOG_VERSION,
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            manifest_key: "target".to_string(),
            namespace: "namespace".to_string(),
            selector_hash: selector,
            shard_hashes: vec![shard],
            created_at_ms: 100,
            expires_at_ms: 200,
            packs: vec![CatalogPackRef {
                digest: digest("pack"),
                pack_bytes: 100,
                entries: vec![entry.clone()],
            }],
            fallback_entries: vec![entry],
        };
        assert!(encode_catalog("", catalog).is_err());
    }

    #[test]
    fn catalog_rejects_wrong_recipe_selector_digest_and_expiry() {
        let shard = digest("shard");
        let selector = selector_hash(
            "target",
            "namespace",
            std::slice::from_ref(&shard),
            crate::cache_key::CACHE_KEY_VERSION,
        )
        .unwrap();
        let base = PackCatalog {
            version: CATALOG_VERSION,
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            manifest_key: "target".to_string(),
            namespace: "namespace".to_string(),
            selector_hash: selector,
            shard_hashes: vec![shard],
            created_at_ms: 100,
            expires_at_ms: 200,
            packs: Vec::new(),
            fallback_entries: Vec::new(),
        };

        let mut wrong_recipe = base.clone();
        wrong_recipe.key_schema -= 1;
        assert!(encode_catalog("", wrong_recipe).is_err());
        let mut wrong_selector = base.clone();
        wrong_selector.selector_hash = digest("wrong");
        assert!(encode_catalog("", wrong_selector).is_err());
        let mut expired = base.clone();
        expired.expires_at_ms = expired.created_at_ms;
        assert!(encode_catalog("", expired).is_err());

        let encoded = encode_catalog("", base).unwrap();
        assert!(decode_catalog(&encoded.bytes, &digest("wrong")).is_err());
    }
}
