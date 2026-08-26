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
pub(crate) const MAX_CATALOG_BYTES: usize = 64 * 1024 * 1024;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogObjectRef {
    pub object_key: String,
    pub created_at_ms: u64,
    pub digest: String,
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
    let ranges = validate_pack_index(&index, payload.len())?;

    let entries = index
        .entries
        .iter()
        .zip(ranges)
        .map(|(entry, range)| DecodedPackEntry {
            descriptor: entry.clone(),
            payload: &payload[range],
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

pub(crate) fn decode_catalog_for_selector(
    bytes: &[u8],
    expected_digest: &str,
    expected_selector: &str,
    now_ms: u64,
) -> Result<PackCatalog> {
    let catalog = decode_catalog(bytes, expected_digest)?;
    if catalog.selector_hash != expected_selector {
        bail!("prefetch catalog does not match the requested selector");
    }
    if catalog.expires_at_ms <= now_ms {
        bail!("prefetch catalog expired at {}", catalog.expires_at_ms);
    }
    Ok(catalog)
}

/// Choose the newest syntactically valid immutable catalog object. Catalog
/// contents remain untrusted until [`decode_catalog_for_selector`] succeeds.
pub(crate) fn latest_catalog_object(
    prefix: &str,
    selector: &str,
    object_keys: &[String],
) -> Result<Option<CatalogObjectRef>> {
    let expected_prefix = catalog_prefix(prefix, selector)?;
    Ok(object_keys
        .iter()
        .filter_map(|key| parse_catalog_object_ref(&expected_prefix, key))
        .max_by(|left, right| {
            (left.created_at_ms, &left.digest).cmp(&(right.created_at_ms, &right.digest))
        }))
}

fn parse_catalog_object_ref(prefix: &str, key: &str) -> Option<CatalogObjectRef> {
    let filename = key.strip_prefix(prefix)?.strip_suffix(".json")?;
    let (created_at, digest) = filename.split_once('-')?;
    if created_at.len() != 20 || !created_at.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if validate_digest("catalog digest", digest).is_err() {
        return None;
    }
    Some(CatalogObjectRef {
        object_key: key.to_string(),
        created_at_ms: created_at.parse().ok()?,
        digest: digest.to_string(),
    })
}

pub(crate) fn decode_catalog_pack<'a>(
    bytes: &'a [u8],
    pack: &CatalogPackRef,
    max_bytes: u64,
) -> Result<DecodedPack<'a>> {
    if bytes.len() as u64 != pack.pack_bytes {
        bail!(
            "prefetch pack size mismatch (catalog {}, object {})",
            pack.pack_bytes,
            bytes.len()
        );
    }
    let decoded = decode_pack(bytes, &pack.digest, max_bytes)?;
    if decoded.entries.len() != pack.entries.len() {
        bail!("prefetch pack entry count does not match its catalog");
    }
    for (decoded, catalog) in decoded.entries.iter().zip(&pack.entries) {
        if decoded.descriptor.cache_key != catalog.cache_key
            || decoded.descriptor.crate_name != catalog.crate_name
            || decoded.descriptor.meta_digest != catalog.meta_digest
        {
            bail!("prefetch pack index does not match its catalog binding");
        }
    }
    Ok(decoded)
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

fn validate_pack_index(
    index: &PackIndex,
    payload_len: usize,
) -> Result<Vec<std::ops::Range<usize>>> {
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
    let mut ranges = Vec::with_capacity(index.entries.len());
    for entry in &index.entries {
        validate_entry_fields(&entry.cache_key, &entry.crate_name, &entry.meta_digest)?;
        if previous_key.is_some_and(|previous| previous >= entry.cache_key.as_str()) {
            bail!("prefetch pack entries are duplicated or not canonically ordered");
        }
        previous_key = Some(&entry.cache_key);
        let Some((next_offset, (start, end))) =
            crate::checked_regions::checked_nonempty_contiguous_region(
                expected_offset,
                entry.offset,
                entry.length,
                payload_len,
            )
        else {
            bail!(
                "prefetch pack entry frames overlap, contain a gap, are empty, or exceed the payload"
            );
        };
        expected_offset = next_offset;
        ranges.push(start..end);
    }
    let payload_len =
        u64::try_from(payload_len).context("prefetch pack payload length overflow")?;
    if expected_offset != payload_len {
        bail!(
            "prefetch pack frame lengths total {expected_offset} bytes, object contains {payload_len}"
        );
    }
    Ok(ranges)
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

    fn catalog_entry(index: usize) -> CatalogEntry {
        CatalogEntry {
            cache_key: digest(&format!("entry-{index:05}")),
            crate_name: "x".to_string(),
            meta_digest: digest(&format!("meta-{index:05}")),
        }
    }

    fn catalog_with(
        packs: Vec<CatalogPackRef>,
        fallback_entries: Vec<CatalogEntry>,
    ) -> PackCatalog {
        let shard = digest("boundary-shard");
        PackCatalog {
            version: CATALOG_VERSION,
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            manifest_key: "target".to_string(),
            namespace: "namespace".to_string(),
            selector_hash: selector_hash(
                "target",
                "namespace",
                std::slice::from_ref(&shard),
                crate::cache_key::CACHE_KEY_VERSION,
            )
            .unwrap(),
            shard_hashes: vec![shard],
            created_at_ms: 100,
            expires_at_ms: 200,
            packs,
            fallback_entries,
        }
    }

    fn index_entries(count: usize) -> Vec<PackIndexEntry> {
        let mut entries = (0..count)
            .map(|index| PackIndexEntry {
                cache_key: digest(&format!("index-{index:05}")),
                crate_name: "x".to_string(),
                meta_digest: digest(&format!("index-meta-{index:05}")),
                offset: 0,
                length: 1,
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.cache_key.cmp(&right.cache_key));
        for (offset, entry) in entries.iter_mut().enumerate() {
            entry.offset = offset as u64;
        }
        entries
    }

    fn framed_index(index: &PackIndex, padded_index_len: usize) -> Vec<u8> {
        let mut json = serde_json::to_vec(index).unwrap();
        assert!(json.len() <= padded_index_len);
        json.resize(padded_index_len, b' ');
        let mut bytes = Vec::with_capacity(PACK_HEADER_BYTES + json.len() + 1);
        bytes.extend_from_slice(PACK_MAGIC);
        bytes.extend_from_slice(&(json.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&json);
        bytes.push(b'x');
        bytes
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
    fn protocol_limits_and_inclusive_boundaries_are_exact() {
        assert_eq!(DEFAULT_MAX_PACK_BYTES, 256 * 1024 * 1024);
        assert_eq!(MAX_PACK_INDEX_BYTES, 16 * 1024 * 1024);
        assert_eq!(MAX_CATALOG_BYTES, 64 * 1024 * 1024);

        assert!(validate_pack_cap(0).is_err());
        assert!(validate_pack_cap(DEFAULT_MAX_PACK_BYTES).is_ok());
        assert!(validate_pack_cap(DEFAULT_MAX_PACK_BYTES + 1).is_err());

        let shard = digest("selector-boundary");
        assert!(
            selector_hash(
                &"a".repeat(MAX_SELECTOR_TEXT_BYTES),
                "n",
                std::slice::from_ref(&shard),
                crate::cache_key::CACHE_KEY_VERSION,
            )
            .is_ok()
        );
        assert!(
            selector_hash(
                &"a".repeat(MAX_SELECTOR_TEXT_BYTES + 1),
                "n",
                std::slice::from_ref(&shard),
                crate::cache_key::CACHE_KEY_VERSION,
            )
            .is_err()
        );
        assert!(
            selector_hash(
                "target",
                "bad\nnamespace",
                std::slice::from_ref(&shard),
                crate::cache_key::CACHE_KEY_VERSION,
            )
            .is_err()
        );
        assert!(
            selector_hash(
                "",
                "namespace",
                &[shard],
                crate::cache_key::CACHE_KEY_VERSION,
            )
            .is_err()
        );
    }

    #[test]
    fn length_prefixes_keep_selector_fields_unambiguous() {
        let without_shards = Vec::new();
        let ab_c = selector_hash(
            "ab",
            "c",
            &without_shards,
            crate::cache_key::CACHE_KEY_VERSION,
        )
        .unwrap();
        let a_bc = selector_hash(
            "a",
            "bc",
            &without_shards,
            crate::cache_key::CACHE_KEY_VERSION,
        )
        .unwrap();
        assert_ne!(ab_c, a_bc);
    }

    #[test]
    fn pack_size_caps_accept_the_limit_and_reject_real_overshoot() {
        let entry = input("cap-boundary", "serde", b"payload");
        let built = build_pack("", vec![entry.clone()], 4096).unwrap();
        let exact = built.bytes.len() as u64;
        assert!(build_pack("", vec![entry.clone()], exact).is_ok());
        assert!(build_pack("", vec![entry], exact - 2).is_err());
        assert!(decode_pack(&built.bytes, &built.digest, exact).is_ok());
        assert!(decode_pack(&built.bytes, &built.digest, exact - 2).is_err());
    }

    #[test]
    fn decoder_checks_magic_index_span_and_index_byte_boundaries_independently() {
        let built = build_pack("", vec![input("magic", "serde", b"x")], 4096).unwrap();
        let mut bad_magic = built.bytes.clone();
        bad_magic[0] ^= 1;
        let digest = blake3::hash(&bad_magic).to_hex().to_string();
        assert!(decode_pack(&bad_magic, &digest, 4096).is_err());

        let mut short_index = Vec::from(PACK_MAGIC.as_slice());
        short_index.extend_from_slice(&1_u64.to_le_bytes());
        let digest = blake3::hash(&short_index).to_hex().to_string();
        assert!(decode_pack(&short_index, &digest, 4096).is_err());
        let truncated_header = &short_index[..PACK_HEADER_BYTES - 1];
        let digest = blake3::hash(truncated_header).to_hex().to_string();
        assert!(decode_pack(truncated_header, &digest, 4096).is_err());

        let index = PackIndex {
            version: PACK_VERSION,
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            entries: index_entries(1),
        };
        let at_limit = framed_index(&index, MAX_PACK_INDEX_BYTES);
        let digest = blake3::hash(&at_limit).to_hex().to_string();
        assert!(decode_pack(&at_limit, &digest, DEFAULT_MAX_PACK_BYTES).is_ok());

        let over_limit = framed_index(&index, MAX_PACK_INDEX_BYTES + 2);
        let digest = blake3::hash(&over_limit).to_hex().to_string();
        assert!(decode_pack(&over_limit, &digest, DEFAULT_MAX_PACK_BYTES).is_err());
    }

    #[test]
    fn pack_index_entry_count_accepts_exact_limit_only() {
        let exact = PackIndex {
            version: PACK_VERSION,
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            entries: index_entries(MAX_PACK_ENTRIES),
        };
        assert!(validate_pack_index(&exact, MAX_PACK_ENTRIES).is_ok());

        let over = PackIndex {
            entries: index_entries(MAX_PACK_ENTRIES + 1),
            ..exact
        };
        assert!(validate_pack_index(&over, MAX_PACK_ENTRIES + 1).is_err());
        let empty = PackIndex {
            version: PACK_VERSION,
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            entries: Vec::new(),
        };
        assert!(validate_pack_index(&empty, 0).is_err());
    }

    #[test]
    fn pack_builder_entry_count_accepts_exact_limit_only() {
        let inputs = |count| {
            (0..count)
                .map(|index| PackInputEntry {
                    cache_key: digest(&format!("build-{index:05}")),
                    crate_name: "x".to_string(),
                    meta_digest: digest(&format!("build-meta-{index:05}")),
                    payload: vec![b'x'],
                })
                .collect::<Vec<_>>()
        };
        let mut exact_index_inputs = inputs(MAX_PACK_ENTRIES);
        let base_index = PackIndex {
            version: PACK_VERSION,
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            entries: exact_index_inputs
                .iter()
                .enumerate()
                .map(|(offset, entry)| PackIndexEntry {
                    cache_key: entry.cache_key.clone(),
                    crate_name: entry.crate_name.clone(),
                    meta_digest: entry.meta_digest.clone(),
                    offset: offset as u64,
                    length: 1,
                })
                .collect(),
        };
        let mut remaining = MAX_PACK_INDEX_BYTES - serde_json::to_vec(&base_index).unwrap().len();
        for entry in &mut exact_index_inputs {
            let extra = remaining.min(127);
            entry.crate_name.push_str(&"x".repeat(extra));
            remaining -= extra;
            if remaining == 0 {
                break;
            }
        }
        assert_eq!(remaining, 0, "entry fields must span the exact index cap");
        let exact = build_pack("", exact_index_inputs.clone(), DEFAULT_MAX_PACK_BYTES).unwrap();
        assert_eq!(
            serde_json::to_vec(&exact.index).unwrap().len(),
            MAX_PACK_INDEX_BYTES
        );
        let expandable = exact_index_inputs
            .iter_mut()
            .find(|entry| entry.crate_name.len() < 128)
            .expect("at least one crate field remains expandable");
        expandable.crate_name.push('x');
        assert!(
            build_pack("", exact_index_inputs, DEFAULT_MAX_PACK_BYTES).is_err(),
            "one byte above the index cap must be rejected"
        );
        assert!(build_pack("", inputs(MAX_PACK_ENTRIES + 1), DEFAULT_MAX_PACK_BYTES,).is_err());
        assert!(build_pack("", inputs(MAX_PACK_ENTRIES + 2), DEFAULT_MAX_PACK_BYTES,).is_err());
    }

    #[test]
    fn catalog_pack_size_and_entry_limits_are_independent() {
        let entry = catalog_entry(0);
        let valid_pack = |pack_bytes, entries| CatalogPackRef {
            digest: digest("boundary-pack"),
            pack_bytes,
            entries,
        };
        assert!(
            encode_catalog(
                "",
                catalog_with(
                    vec![valid_pack(DEFAULT_MAX_PACK_BYTES, vec![entry.clone()])],
                    Vec::new(),
                ),
            )
            .is_ok()
        );
        assert!(
            encode_catalog(
                "",
                catalog_with(vec![valid_pack(0, vec![entry.clone()])], Vec::new()),
            )
            .is_err()
        );
        assert!(
            encode_catalog(
                "",
                catalog_with(
                    vec![valid_pack(DEFAULT_MAX_PACK_BYTES + 1, vec![entry])],
                    Vec::new(),
                ),
            )
            .is_err()
        );

        let exact_entries = (0..MAX_PACK_ENTRIES).map(catalog_entry).collect();
        assert!(
            encode_catalog(
                "",
                catalog_with(vec![valid_pack(1, exact_entries)], Vec::new()),
            )
            .is_ok()
        );
        let over_entries = (0..=MAX_PACK_ENTRIES).map(catalog_entry).collect();
        assert!(
            encode_catalog(
                "",
                catalog_with(vec![valid_pack(1, over_entries)], Vec::new()),
            )
            .is_err()
        );
        assert!(
            encode_catalog(
                "",
                catalog_with(vec![valid_pack(1, Vec::new())], Vec::new()),
            )
            .is_err()
        );
    }

    #[test]
    fn catalog_total_entry_and_pack_count_limits_are_inclusive() {
        let exact_fallback = (0..MAX_PACK_ENTRIES).map(catalog_entry).collect();
        assert!(encode_catalog("", catalog_with(Vec::new(), exact_fallback)).is_ok());
        let over_fallback = (0..=MAX_PACK_ENTRIES).map(catalog_entry).collect();
        assert!(encode_catalog("", catalog_with(Vec::new(), over_fallback)).is_err());

        let packs = (0..MAX_CATALOG_PACKS)
            .map(|index| CatalogPackRef {
                digest: digest(&format!("pack-{index}")),
                pack_bytes: 1,
                entries: vec![catalog_entry(index)],
            })
            .collect::<Vec<_>>();
        assert!(encode_catalog("", catalog_with(packs.clone(), Vec::new())).is_ok());
        let mut over = packs;
        over.push(CatalogPackRef {
            digest: digest("pack-over-limit"),
            pack_bytes: 1,
            entries: vec![catalog_entry(MAX_CATALOG_PACKS)],
        });
        assert!(encode_catalog("", catalog_with(over, Vec::new())).is_err());
    }

    #[test]
    fn catalog_decoder_accepts_exact_byte_limit_and_rejects_overshoot() {
        let encoded = encode_catalog("", catalog_with(Vec::new(), vec![catalog_entry(0)])).unwrap();
        let mut bytes = encoded.bytes;
        bytes.resize(MAX_CATALOG_BYTES, b' ');
        let digest = blake3::hash(&bytes).to_hex().to_string();
        assert!(decode_catalog(&bytes, &digest).is_ok());

        bytes.push(b' ');
        let digest = blake3::hash(&bytes).to_hex().to_string();
        assert!(decode_catalog(&bytes, &digest).is_err());
    }

    #[test]
    fn catalog_pack_binding_rejects_each_mismatch_independently() {
        let built = build_pack("", vec![input("binding", "serde", b"payload")], 4096).unwrap();
        let descriptor = &built.index.entries[0];
        let base = CatalogPackRef {
            digest: built.digest.clone(),
            pack_bytes: built.bytes.len() as u64,
            entries: vec![CatalogEntry {
                cache_key: descriptor.cache_key.clone(),
                crate_name: descriptor.crate_name.clone(),
                meta_digest: descriptor.meta_digest.clone(),
            }],
        };
        assert!(decode_catalog_pack(&built.bytes, &base, 4096).is_ok());

        let mut wrong_crate = base.clone();
        wrong_crate.entries[0].crate_name = "tokio".to_string();
        assert!(decode_catalog_pack(&built.bytes, &wrong_crate, 4096).is_err());
        let mut wrong_meta = base.clone();
        wrong_meta.entries[0].meta_digest = digest("wrong-meta");
        assert!(decode_catalog_pack(&built.bytes, &wrong_meta, 4096).is_err());
        let mut wrong_key = base.clone();
        wrong_key.entries[0].cache_key = digest("wrong-key");
        assert!(decode_catalog_pack(&built.bytes, &wrong_key, 4096).is_err());
        let mut wrong_count = base.clone();
        wrong_count.entries.push(catalog_entry(99));
        assert!(decode_catalog_pack(&built.bytes, &wrong_count, 4096).is_err());
        let mut wrong_size = base;
        wrong_size.pack_bytes += 1;
        assert!(decode_catalog_pack(&built.bytes, &wrong_size, 4096).is_err());
    }

    #[test]
    fn catalog_rejects_an_individually_unsafe_fallback_entry() {
        let mut unsafe_entry = catalog_entry(0);
        unsafe_entry.crate_name = "../escape".to_string();
        assert!(encode_catalog("", catalog_with(Vec::new(), vec![unsafe_entry])).is_err());
    }

    #[test]
    fn catalog_discovery_filters_bad_timestamp_shape_and_digits_separately() {
        let selector = digest("discovery-selector");
        let prefix = catalog_prefix("root", &selector).unwrap();
        let valid_digest = digest("catalog");
        let valid = format!("{prefix}{:020}-{valid_digest}.json", 7);
        let bad_width = format!("{prefix}7-{valid_digest}.json");
        let bad_digits = format!("{prefix}0000000000000000000x-{valid_digest}.json");
        assert!(parse_catalog_object_ref(&prefix, &bad_width).is_none());
        assert!(parse_catalog_object_ref(&prefix, &bad_digits).is_none());
        let latest =
            latest_catalog_object("root", &selector, &[bad_width, bad_digits, valid.clone()])
                .unwrap()
                .unwrap();
        assert_eq!(latest.object_key, valid);
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
