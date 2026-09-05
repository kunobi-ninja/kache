use kache_format::{CachedFile, EntryMeta, is_blob_hash, is_valid_cache_key};

#[test]
fn entry_v31_retains_its_serialized_representation() {
    let fixture = include_str!("fixtures/entry-v31.json").trim_end();
    let entry: EntryMeta = serde_json::from_str(fixture).unwrap();

    assert_eq!(entry.key_schema, 31);
    assert_eq!(entry.compile_time_ms, 1200);
    assert_eq!(entry.stderr, "warning: example\n");
    assert!(entry.files[0].executable);
    assert_eq!(entry.files[0].size, 4);
    assert_eq!(serde_json::to_string_pretty(&entry).unwrap(), fixture);
}

#[test]
fn legacy_entries_keep_their_defaults() {
    let entry: EntryMeta = serde_json::from_str(
        r#"{"cache_key":"legacy","crate_name":"hello","crate_types":["lib"],
            "files":[{"name":"libhello.rlib","size":4,"hash":"legacy"}],
            "stdout":"","stderr":""}"#,
    )
    .unwrap();

    assert_eq!(entry.key_schema, 0);
    assert_eq!(entry.compile_time_ms, 0);
    assert!(entry.features.is_empty());
    assert!(entry.target.is_empty());
    assert!(entry.profile.is_empty());
    assert!(entry.emit_kinds.is_empty());
    assert!(!entry.files[0].executable);
    assert!(entry.covers_requested_emit(&["link".into()]));
}

#[test]
fn missing_required_fields_still_fail_to_decode() {
    assert!(serde_json::from_str::<EntryMeta>(r#"{"cache_key":"key"}"#).is_err());
    assert!(serde_json::from_str::<CachedFile>(r#"{"name":"lib.rlib","size":4}"#).is_err());
}

#[test]
fn cache_keys_and_legacy_blob_hashes_keep_their_distinct_case_rules() {
    let lowercase = "0123456789abcdef".repeat(4);
    let uppercase = lowercase.to_ascii_uppercase();
    assert!(is_valid_cache_key(&lowercase));
    assert!(is_blob_hash(&lowercase));
    assert!(!is_valid_cache_key(&uppercase));
    assert!(is_blob_hash(&uppercase));
}
