use super::{
    Config, EntryMeta, EventInputs, EventResult, FileHashStats, KeyEventRecord, log_event,
    print_progress, replay_cached_diagnostics,
};
use std::time::Instant;

/// Reporting facts supplied only after the compiler's restore succeeds.
pub(super) struct HitCompletion<'a> {
    pub event_root: &'a str,
    pub crate_name: &'a str,
    pub result: EventResult,
    pub cache_key: &'a str,
    pub start: Instant,
    pub key_ms: u64,
    pub key_hash_stats: FileHashStats,
    pub lookup_ms: u64,
    pub restore_ms: u64,
    /// What the hit's key recorded for the event; empty for C and C++.
    pub key_record: KeyEventRecord,
}

impl HitCompletion<'_> {
    /// Emit the completed hit and replay its diagnostics once. Compiler-specific
    /// work, such as memo commits and incremental cleanup, stays with the caller.
    pub(super) fn report(self, config: &Config, meta: &EntryMeta) {
        let elapsed = self.start.elapsed().as_millis() as u64;
        let size = meta.files.iter().map(|file| file.size).sum();
        log_event(
            config,
            EventInputs::new(self.event_root, self.crate_name, self.result, elapsed)
                .compile_time_ms(meta.compile_time_ms)
                .size(size)
                .keyed(self.cache_key, self.key_ms, self.key_hash_stats)
                .lookup_ms(self.lookup_ms)
                .restore_ms(self.restore_ms)
                .key_record(self.key_record),
        );
        print_progress(self.crate_name, self.result, elapsed, size);
        replay_cached_diagnostics(meta, std::io::stdout(), std::io::stderr());
    }
}
