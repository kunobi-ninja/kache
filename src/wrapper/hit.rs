use super::{
    Config, EntryMeta, EventResult, FileHashStats, log_event_with_hash_stats, print_progress,
    replay_cached_diagnostics,
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
}

impl HitCompletion<'_> {
    /// Emit the completed hit and replay its diagnostics once. Compiler-specific
    /// work, such as memo commits and incremental cleanup, stays with the caller.
    pub(super) fn report(self, config: &Config, meta: &EntryMeta) {
        let elapsed = self.start.elapsed().as_millis() as u64;
        let size = meta.files.iter().map(|file| file.size).sum();
        log_event_with_hash_stats(
            config,
            self.event_root,
            self.crate_name,
            self.result,
            elapsed,
            meta.compile_time_ms,
            size,
            self.cache_key,
            self.key_ms,
            self.key_hash_stats,
            self.lookup_ms,
            self.restore_ms,
            0,
        );
        print_progress(self.crate_name, self.result, elapsed, size);
        replay_cached_diagnostics(meta, std::io::stdout(), std::io::stderr());
    }
}
