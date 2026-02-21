// Phase 4: Why-miss diff engine
//
// Compares a missed cache_key's current inputs against the last known good
// inputs from cache_key_inputs table. Reports which fields changed:
// - rustc_version changed
// - source_hash changed (code modified)
// - extern dependency hash changed (upstream dep recompiled)
// - features changed
// - rustflags changed
